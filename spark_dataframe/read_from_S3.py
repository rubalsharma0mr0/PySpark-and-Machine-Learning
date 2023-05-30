from datetime import datetime

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("Read From S3").master("local[*]")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,"
                                       "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0,"
                                       "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,"
                                       "org.postgresql:postgresql:42.4.1,"
                                       "com.oracle.database.jdbc:ojdbc8:21.5.0.0,"
                                       "com.microsoft.sqlserver:mssql-jdbc:9.2.1.jre8,"
                                       "com.github.housepower:clickhouse-native-jdbc:2.6.5,"
                                       "org.apache.hadoop:hadoop-aws:3.2.0")
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
        .config("spark.driver.bindAddress", "172.16.16.51")
        .config("spark.driver.host", "172.16.16.51")
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
        # .config("spark.dynamicAllocation.minExecutors", "20")
        # .config("spark.dynamicAllocation.maxExecutors", "20")
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .getOrCreate())

    start = datetime.now()
    print(f"Start Time: {start}")

    df = (spark.read.format('parquet')
          .option("header", "true")
          .option("fs.s3a.access.key", "")
          .option("fs.s3a.secret.key", "")
          .option("fs.s3a.endpoint", "")
          .option("fs.s3a.connection.timeout", "600000")
          .option("fs.s3a.connection.ssl.enabled", "true")
          .option("fs.s3a.path.style.access", "true")
          .option("fs.s3a.attempts.maximum", "10")
          .option("fs.s3a.connection.establish.timeout", "5000")
          .option("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
          .load("s3a://testqa/accounts_bio_1crs"))

    print(f"Count: {df.count()}")
    df.collect()
    print(f"End Time: {str(datetime.now() - start)}")
