import logging
import os

import pyspark.sql.functions as sf
import pyspark.sql.types as st
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

SOURCE_PATH = "s3a://prd-lake-bedrock-pokeboll/pokemons_details/"
DESTINATION_PATH = "s3a://prd-lake-iron-pokeboll/pokemons/"


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


def build_pokemon_details_schema() -> st.StructType:
    """Constrói o schema Spark para o payload retornado por PokeApiClient.fetch_one_pokemon().

    Fonte: https://pokeapi.co/docs/v2#pokemon
    Endpoint: GET /api/v2/pokemon/{id or name}/

    Returns:
        StructType com o schema completo do recurso Pokemon.
    """
    named_api_resource = st.StructType(
        [
            st.StructField("name", st.StringType(), True),
            st.StructField("url", st.StringType(), True),
        ]
    )

    pokemon_ability = st.StructType(
        [
            st.StructField("is_hidden", st.BooleanType(), True),
            st.StructField("slot", st.IntegerType(), True),
            st.StructField("ability", named_api_resource, True),
        ]
    )

    version_game_index = st.StructType(
        [
            st.StructField("game_index", st.IntegerType(), True),
            st.StructField("version", named_api_resource, True),
        ]
    )

    pokemon_held_item_version = st.StructType(
        [
            st.StructField("version", named_api_resource, True),
            st.StructField("rarity", st.IntegerType(), True),
        ]
    )

    pokemon_held_item = st.StructType(
        [
            st.StructField("item", named_api_resource, True),
            st.StructField(
                "version_details",
                st.ArrayType(pokemon_held_item_version),
                True,
            ),
        ]
    )

    pokemon_move_version = st.StructType(
        [
            st.StructField("move_learn_method", named_api_resource, True),
            st.StructField("version_group", named_api_resource, True),
            st.StructField("level_learned_at", st.IntegerType(), True),
            st.StructField("order", st.IntegerType(), True),
        ]
    )

    pokemon_move = st.StructType(
        [
            st.StructField("move", named_api_resource, True),
            st.StructField(
                "version_group_details",
                st.ArrayType(pokemon_move_version),
                True,
            ),
        ]
    )

    pokemon_stat = st.StructType(
        [
            st.StructField("stat", named_api_resource, True),
            st.StructField("effort", st.IntegerType(), True),
            st.StructField("base_stat", st.IntegerType(), True),
        ]
    )

    pokemon_type = st.StructType(
        [
            st.StructField("slot", st.IntegerType(), True),
            st.StructField("type", named_api_resource, True),
        ]
    )

    pokemon_type_past = st.StructType(
        [
            st.StructField("generation", named_api_resource, True),
            st.StructField("types", st.ArrayType(pokemon_type), True),
        ]
    )

    sprite_set = st.StructType(
        [
            st.StructField("front_default", st.StringType(), True),
            st.StructField("front_shiny", st.StringType(), True),
            st.StructField("front_female", st.StringType(), True),
            st.StructField("front_shiny_female", st.StringType(), True),
        ]
    )

    pokemon_sprites = st.StructType(
        [
            st.StructField("front_default", st.StringType(), True),
            st.StructField("front_shiny", st.StringType(), True),
            st.StructField("front_female", st.StringType(), True),
            st.StructField("front_shiny_female", st.StringType(), True),
            st.StructField("back_default", st.StringType(), True),
            st.StructField("back_shiny", st.StringType(), True),
            st.StructField("back_female", st.StringType(), True),
            st.StructField("back_shiny_female", st.StringType(), True),
            st.StructField(
                "other",
                st.StructType(
                    [
                        st.StructField("dream_world", sprite_set, True),
                        st.StructField("home", sprite_set, True),
                        st.StructField("official-artwork", sprite_set, True),
                        st.StructField("showdown", sprite_set, True),
                    ]
                ),
                True,
            ),
        ]
    )

    pokemon_cries = st.StructType(
        [
            st.StructField("latest", st.StringType(), True),
            st.StructField("legacy", st.StringType(), True),
        ]
    )

    return st.StructType(
        [
            st.StructField("id", st.IntegerType(), True),
            st.StructField("name", st.StringType(), True),
            st.StructField("base_experience", st.IntegerType(), True),
            st.StructField("height", st.IntegerType(), True),
            st.StructField("weight", st.IntegerType(), True),
            st.StructField("order", st.IntegerType(), True),
            st.StructField("is_default", st.BooleanType(), True),
            st.StructField("location_area_encounters", st.StringType(), True),
            st.StructField("abilities", st.ArrayType(pokemon_ability), True),
            st.StructField("forms", st.ArrayType(named_api_resource), True),
            st.StructField("game_indices", st.ArrayType(version_game_index), True),
            st.StructField("held_items", st.ArrayType(pokemon_held_item), True),
            st.StructField("moves", st.ArrayType(pokemon_move), True),
            st.StructField("stats", st.ArrayType(pokemon_stat), True),
            st.StructField("types", st.ArrayType(pokemon_type), True),
            st.StructField("past_types", st.ArrayType(pokemon_type_past), True),
            st.StructField("species", named_api_resource, True),
            st.StructField("sprites", pokemon_sprites, True),
            st.StructField("cries", pokemon_cries, True),
        ]
    )


def read_bronze_pokemon_details(spark_session: SparkSession, path: str) -> DataFrame:
    """Lê os detalhes de Pokémons da camada bronze no formato Delta Lake.

    Args:
        spark_session: Sessão Spark usada para leitura.
        path: Caminho do Delta Lake de origem.

    Returns:
        DataFrame com a coluna `data` contendo os detalhes em JSON.
    """
    df = spark_session.read.format("delta").load(path)
    logger.info("Detalhes de Pokémons lidos da camada bronze: %s", path)
    return df


def parse_pokemon_details(df: DataFrame) -> DataFrame:
    """Faz o parse da coluna JSON `data` e expande os campos como colunas tipadas.

    Args:
        df: DataFrame com a coluna `data` contendo os detalhes em JSON.

    Returns:
        DataFrame com os campos do Pokemon expandidos e tipados.
    """
    schema = build_pokemon_details_schema()
    return (
        df.withColumn("data_struct", sf.from_json(sf.col("data"), schema))
        .select("data_struct.*")
    )


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
    except IOError:
        logger.exception("Erro ao salvar DataFrame em %s", path)
        raise


def main() -> None:
    """Ponto de entrada da pipeline iron de Pokémons."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    logger.info("Iniciando pipeline iron de Pokémons")

    spark_session = build_spark_session("iron_pokemons")

    df_bronze = read_bronze_pokemon_details(spark_session, SOURCE_PATH)
    df_iron = parse_pokemon_details(df_bronze)
    save_dataframe(df_iron, DESTINATION_PATH)

    logger.info("Pipeline finalizada com sucesso")


if __name__ == "__main__":
    main()
