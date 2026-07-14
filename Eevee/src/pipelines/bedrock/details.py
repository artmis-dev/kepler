import json
import logging
import os

from pyspark.sql import DataFrame, Row, SparkSession

from src.models.PokeApiClient import PokeApiClient

logger = logging.getLogger(__name__)

SOURCE_PATH = "s3a://prd-lake-bedrock-pokeboll/pokemons/"
DESTINATION_PATH = "s3a://prd-lake-bedrock-pokeboll/pokemons_details/"


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


def get_pokemon_names_from_lake(spark_session: SparkSession, path: str) -> list[str]:
    """Lê o Delta Lake de Pokémons e retorna a lista de nomes.

    Args:
        spark_session: Sessão Spark usada para ler o Delta Lake.
        path: Caminho do Delta Lake de origem.

    Returns:
        Lista de nomes de Pokémons.
    """
    df = spark_session.read.format("delta").load(path)
    names = [row["name"] for row in df.select("name").collect()]
    logger.info("Pokémons lidos do lake: %d", len(names))
    return names


def get_pokemon_details_from_api(
    spark_session: SparkSession,
    poke_client: PokeApiClient,
    names: list[str],
) -> DataFrame:
    """Busca detalhes de cada Pokémon na PokeAPI e retorna um DataFrame com os dados brutos.

    Args:
        spark_session: Sessão Spark usada para criar o DataFrame.
        poke_client: Cliente usado para consultar a PokeAPI.
        names: Lista de nomes de Pokémons a serem consultados.

    Returns:
        DataFrame do Spark com os dados brutos dos Pokémons retornados pela API.
    """
    details: list[Row] = []

    for name in names:
        raw = poke_client.fetch_one_pokemon(name)
        if raw:
            details.append(Row(data=json.dumps(raw, ensure_ascii=False)))
            logger.info("Detalhes coletados: %s", name)

    logger.info("Total de detalhes coletados: %d", len(details))
    return spark_session.createDataFrame(details)


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
    """Ponto de entrada da pipeline bronze de detalhes de Pokémons."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    logger.info("Iniciando pipeline bronze de detalhes de Pokémons")

    spark_session = build_spark_session("bronze_pokemon_details")
    poke_client = PokeApiClient()

    names = get_pokemon_names_from_lake(spark_session, SOURCE_PATH)
    df = get_pokemon_details_from_api(spark_session, poke_client, names)
    save_dataframe(df, DESTINATION_PATH)

    logger.info("Pipeline finalizada com sucesso")


if __name__ == "__main__":
    main()
