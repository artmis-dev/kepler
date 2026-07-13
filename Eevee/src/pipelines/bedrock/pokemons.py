import logging
import os

from pyspark.sql import DataFrame, SparkSession

from src.models.PokeApiClient import PokeApiClient

logger = logging.getLogger(__name__)

MAX_OFFSET = 200
PAGE_SIZE = 60

DESTINATION_PATH = "s3a://prd-lake-bedrock-pokeboll/pokemons/"


def _get_required_env(name: str) -> str:
    """Retorna o valor de uma variável de ambiente obrigatória.

    Args:
        name: Nome da variável de ambiente.

    Returns:
        Valor da variável.

    Raises:
        EnvironmentError: Se a variável não estiver definida.
    """
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


def build_spark_session(app_name: str) -> SparkSession:
    """Cria e configura uma SparkSession com suporte a Delta Lake e S3.

    Args:
        app_name: Nome da aplicação Spark.

    Returns:
        SparkSession configurada.
    """
    endpoint = _get_required_env("MAGALU_ENDPOINT")
    access_key = _get_required_env("MAGALU_ACCESS_KEY")
    secret_key = _get_required_env("MAGALU_SECRET_KEY")

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_pokemons_from_api(
    spark_session: SparkSession,
    poke_client: PokeApiClient,
) -> DataFrame:
    """Busca todos os Pokémons paginando a PokeAPI e retorna um DataFrame.

    Args:
        spark_session: Sessão Spark usada para criar o DataFrame.
        poke_client: Cliente usado para consultar a PokeAPI.

    Returns:
        DataFrame do Spark contendo os Pokémons retornados pela API.
    """
    pokemons: list[dict] = []
    offset = 0

    while offset <= MAX_OFFSET:
        batch = poke_client.fetch_all_pokemons(offset=offset)
        logger.info(
            "Pokémons coletados — offset=%d, registros=%d",
            offset,
            len(batch),
        )
        pokemons.extend(batch)
        offset += PAGE_SIZE

    logger.info("Total de Pokémons coletados: %d", len(pokemons))
    return spark_session.createDataFrame(pokemons)


def save_dataframe(df: DataFrame, path: str) -> None:
    """Persiste um DataFrame no formato Delta Lake.

    Args:
        df: DataFrame a ser salvo.
        path: Caminho de destino (ex.: s3a://bucket/path/).

    Raises:
        IOError: Se ocorrer falha na escrita.
    """
    try:
        df.write.format("delta").mode("overwrite").save(path)
        logger.info("DataFrame salvo com sucesso em %s", path)
    except IOError as error:
        logger.exception("Erro ao salvar DataFrame em %s", path)
        raise


def main() -> None:
    """Ponto de entrada da pipeline bronze de Pokémons."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    logger.info("Iniciando pipeline bronze de Pokémons")

    spark_session = build_spark_session("bronze_pokemons")
    poke_client = PokeApiClient()

    df = get_pokemons_from_api(spark_session, poke_client)
    save_dataframe(df, DESTINATION_PATH)

    logger.info("Pipeline finalizada com sucesso")


if __name__ == "__main__":
    main()
