from datetime import datetime

from pyspark.sql import SparkSession

if __name__ == '__main__':
    master_url = ''
    # Create a SparkSession
    spark = (
        SparkSession.builder.appName(f"Read From S3: {datetime.now()}").master(f"spark://{master_url}")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,"
                                       "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0,"
                                       "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,"
                                       "org.postgresql:postgresql:42.4.1,"
                                       "com.oracle.database.jdbc:ojdbc8:21.5.0.0,"
                                       "com.microsoft.sqlserver:mssql-jdbc:9.2.1.jre8,"
                                       "com.github.housepower:clickhouse-native-jdbc:2.6.5,"
                                       "org.apache.hadoop:hadoop-aws:3.3.4,"
                                       "org.apache.spark:spark-hadoop-cloud_2.12:3.4.0")
        .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")
        .config("spark.driver.memory", "4G")
        .config("spark.executor.memory", "8G")
        .config("spark.executor.cores", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.driver.bindAddress", "172.16.17.143")
        .config("spark.driver.host", "172.16.17.143")
        .config("spark.driver.port", "0")
        .config("spark.ui.port", "4040")
        .config("spark.cassandra.connection.localConnectionsPerExecutor", "10")
        .config("spark.cassandra.output.consistency.level", "LOCAL_ONE")
        .config("spark.cores.max", "40")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
        .config("spark.dynamicAllocation.executorIdleTimeout", '60')
        .config("spark.dynamicAllocation.schedularBacklogTimeout", '1')
        .config("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")
        .config("spark.dynamicAllocation.minExecutors", "3")
        .config("spark.dynamicAllocation.maxExecutors", "4")
        # Config for S3 and MinIO
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .config("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true")
        .getOrCreate())

    # Start the job timer
    start = datetime.now()
    print(f"Start Job: {start}")

    # Read data from parquet format in S3
    df = (spark.read.format('parquet')
          .option("header", "true")
          .option("fs.s3a.access.key", "")  # S3 access key
          .option("fs.s3a.secret.key", "")  # S3 secret key
          .option("fs.s3a.endpoint", "http://")  # S3 endpoint
          .option("fs.s3a.connection.timeout", "600000")  # S3 connection timeout
          .option("fs.s3a.connection.ssl.enabled", "true")  # Enable SSL
          .option("fs.s3a.path.style.access", "true")  # Use path-style access
          .option("fs.s3a.attempts.maximum", "10")  # Maximum number of connection attempts
          .option("fs.s3a.connection.establish.timeout", "5000")  # S3 connection establishment timeout
          .option("spark.hadoop.fs.s3a.impl",
                  "org.apache.hadoop.fs.s3a.S3AFileSystem")  # Use S3A file system implementation
          .load("s3a://"))  # S3 path to load data from

    duplicated_df = df
    for _ in range(8):
        # Union of the data frame with itself 8 times
        duplicated_df = duplicated_df.unionAll(df)

    df = duplicated_df

    # Print the count of rows in the data frame
    print(f"Count: {df.count()}")

    # Start writing the data frame to S3
    start_write = datetime.now()
    print(f"Write start: {str(start_write)}")

    write_format = "csv"

    (df
     # .coalesce(1)  # Optionally, coalesce the data frame to a single partition for writing
     .write
     .format(write_format)
     .mode('overwrite')
     .option("header", "true")
     .option("fs.s3a.access.key", "")  # S3 access key
     .option("fs.s3a.secret.key", "")  # S3 secret key
     .option("fs.s3a.path.style.access", "true")  # Use path-style access
     .option("fs.s3a.endpoint", "http://")  # S3 endpoint
     .save("s3a://"))  # S3 path to save the data frame to

    # Print the duration of the writing process
    print(f"Write End: {str(datetime.now() - start_write)}")

    # Print the total duration of the job
    print(f"End Job: {str(datetime.now() - start)}")
