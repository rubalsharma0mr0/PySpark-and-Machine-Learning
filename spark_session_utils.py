import os

from logger import Logger

dependencies_list = [
    # MongoDB Spark Connector for connecting to MongoDB databases.
    "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
    # Microsoft Azure SQL Connector for connecting to Azure SQL databases.
    "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0",
    # DataStax Cassandra Connector for connecting to Apache Cassandra databases.
    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0",
    # PostgreSQL JDBC driver for connecting to PostgreSQL databases.
    "org.postgresql:postgresql:42.4.1",
    # Oracle JDBC driver for connecting to Oracle databases.
    "com.oracle.database.jdbc:ojdbc8:21.5.0.0",
    # Microsoft SQL Server JDBC driver for connecting to SQL Server databases.
    "com.microsoft.sqlserver:mssql-jdbc:9.2.1.jre8",
    # ClickHouse Native JDBC driver for connecting to ClickHouse databases.
    "com.github.housepower:clickhouse-native-jdbc:2.6.5",
    # Hadoop AWS library for accessing AWS services like S3 for data storage.
    "org.apache.hadoop:hadoop-aws:3.3.4",
    # Spark Hadoop Cloud library for integrating Spark with Hadoop and cloud services.
    "org.apache.spark:spark-hadoop-cloud_2.12:3.4.0",
    # Apache Iceberg library for iceberg data format support in Spark.
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0",
]


def get_spark_catalog_info_dict() -> dict:
    Logger.log().debug("Getting Spark Catalog")
    minio_endpoint = os.getenv("ENDPOINT")
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")
    hive_metastore_url = os.getenv("HIVE_METASTORE_URL")
    # Define the custom catalog name.
    catalog_name = "custom_catalog"

    # Configure the Spark session with Iceberg related settings.
    return {
        # Specify the Iceberg Spark session extension.
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        # Specify the catalog implementation for Iceberg, in this case, it's SparkCatalog.
        f"spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
        # Set the type of the catalog to "hive" for Hive compatibility.
        f"spark.sql.catalog.spark_catalog.type": "hive",
        # Define the URI for the Iceberg catalog, typically for Hive it's Thrift URI.
        f"spark.sql.catalog.spark_catalog.uri": f"{hive_metastore_url}",
        # Configure the custom catalog's I/O implementation (e.g., for S3).
        f"spark.sql.catalog.{catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        # Define the custom catalog itself for Iceberg integration.
        f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkSessionCatalog",
        # Set AWS S3 access key for the custom catalog's interactions with S3.
        "spark.hadoop.fs.s3a.access.key": f"{access_key}",
        # Set AWS S3 secret key for the custom catalog's interactions with S3.
        "spark.hadoop.fs.s3a.secret.key": f"{secret_key}",
        # Define the endpoint for the custom catalog to communicate with S3 (e.g., Minio).
        "spark.hadoop.fs.s3a.endpoint": f"{minio_endpoint}",
    }


def get_spark_session_extra_properties(spark_session_builder) -> None:
    """
    Configures extra Spark properties for a Spark session based on the 'SPARK_EXTRA_SPARK_PROPERTIES'
    environment variable.

    Args:
        spark_session_builder: The SparkSession builder object to configure.

    Raises:
        PysparkEngineException: If an exception occurs during the configuration process.
    """
    try:
        # Check if 'SPARK_EXTRA_SPARK_PROPERTIES' is defined in the environment variables
        if "SPARK_EXTRA_SPARK_PROPERTIES" in os.environ:
            # Get the value of 'SPARK_EXTRA_SPARK_PROPERTIES' from the environment
            spark_extra_properties = os.getenv("SPARK_EXTRA_SPARK_PROPERTIES", '').strip()

            # Check if the value is non-empty
            if spark_extra_properties:
                spark_extra_property_map = {}

                # Iterate through key-value pairs separated by commas
                for prop in spark_extra_properties.split(","):
                    # Split key and value, and remove leading/trailing whitespaces
                    key, value = map(str.strip, prop.split("="))

                    # Check if both key and value are present
                    if key and value:
                        # Check for duplicate keys and handle accordingly
                        if key not in spark_extra_property_map:
                            spark_extra_property_map[key] = value
                        else:
                            # Log a warning for duplicate keys
                            Logger.log().warning(f'Duplicate key "{key}" detected; '
                                                 f'only the first occurrence will be considered.')
                    else:
                        # Log a warning for invalid key-value pairs
                        Logger.log().warning(f'The key-value pair "{prop}" is not valid and will be ignored.')

                # Configure Spark session with the extra properties map
                spark_session_builder.config(map=spark_extra_property_map)
            else:
                # Log a warning if 'SPARK_EXTRA_SPARK_PROPERTIES' is empty
                Logger.log().warning("'SPARK_EXTRA_SPARK_PROPERTIES' environment variable is empty.")
    except Exception as exp:
        # Log an error and raise a custom exception if any other exception occurs
        Logger.log().error(f"Exception caught: {exp}")


class SparkSessionUtils:

    # create new Spark Session
    def __init__(self, app_name):
        self.session = None
        self.properties = None
        self.app_name = app_name

    def __create_spark_session(self, ):
        from pyspark.sql import SparkSession

        Logger.log().debug("Creating Spark Session")

        spark_session_builder = (
            SparkSession.builder.appName("SPARK SHARED CONTEXT")
            .master('local[*]')
            # .config("spark.jars.packages", ",".join(dependencies_list))
            # .config(
            #     "spark.driver.memory",
            #     os.getenv('SPARK_DRIVER_MEMORY'),
            # )
            # .config(
            #     "spark.executor.memory",
            #     os.getenv('SPARK_EXECUTOR_MEMORY'),
            # )
            # .config(
            #     "spark.executor.cores",
            #     os.getenv('SPARK_EXECUTOR_CORE'),
            # )
            # .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            # .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            # .config("spark.sql.adaptive.enabled", "true")
            # .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            # .config("spark.sql.shuffle.partitions", "10")
            # .config("spark.scheduler.mode", "FAIR")
            # .config(
            #     "spark.driver.bindAddress",
            #     os.getenv('SPARK_DRIVER_BINDADDRESS'),
            # )
            # .config(
            #     "spark.driver.host", os.getenv('SPARK_DRIVER_HOST')
            # )
            # .config(
            #     "spark.driver.port", os.getenv('SPARK_DRIVER_PORT')
            # )
            # .config("spark.ui.port", os.getenv('SPARK_UI_PORT'))
            # .config("spark.cassandra.connection.localConnectionsPerExecutor", "10")
            # .config("spark.cores.max", "40")
            # .config("spark.dynamicAllocation.enabled", "true")
            # .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            # .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
            # .config("spark.dynamicAllocation.executorIdleTimeout", "60")
            # .config("spark.dynamicAllocation.schedularBacklogTimeout", "1")
            # .config("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")
            # .config(
            #     "spark.dynamicAllocation.minExecutors",
            #     os.getenv('SPARK_MIN_EXECUTORS'),
            # )
            # .config(
            #     "spark.dynamicAllocation.maxExecutors",
            #     os.getenv('SPARK_MAX_EXECUTORS'),
            # )
            # .config(
            #     "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            # )
            # .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            # .config("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true")
            # .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        )
        get_spark_session_extra_properties(spark_session_builder)

        try:
            hive_metastore_url = os.getenv("HIVE_METASTORE_URL")
            if hive_metastore_url and hive_metastore_url != "":
                self.session = (
                    spark_session_builder.config(
                        "hive.metastore.uris",
                        os.getenv('HIVE_METASTORE_URL'),
                    )
                    .config(map=get_spark_catalog_info_dict())
                    .enableHiveSupport()
                    .getOrCreate()
                )
            else:
                self.session = spark_session_builder.getOrCreate()
        except Exception as exp:
            Logger.log().error(f"Error occurred while creating spark session: {exp}")

        spark_log_level = os.getenv("SPARK_LOG_LEVEL")
        if spark_log_level and spark_log_level != "":
            self.session.sparkContext.setLogLevel(spark_log_level)
        return self.session

    # Get Spark Session
    def get_spark_session(self):
        if self.session is not None:
            return self.session
        else:
            return self.__create_spark_session()

    # close Spark Session
    @staticmethod
    def close_spark_session(spark):
        if spark is not None:
            spark.stop()
        else:
            return False
