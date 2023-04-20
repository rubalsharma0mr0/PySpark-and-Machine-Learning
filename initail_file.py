from pyspark.shell import spark

df = spark.read.option('header', 'true').csv('mydata.csv')
df.show()
