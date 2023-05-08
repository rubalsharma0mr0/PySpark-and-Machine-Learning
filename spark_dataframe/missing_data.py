from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

spark = SparkSession.builder.appName("Groupby and Aggregate Operations").getOrCreate()

df = spark.read.csv('../data_source/ContainsNull.csv', inferSchema=True, header=True)
df.printSchema()
df.show()

print("Drop row with null values")
df.na.drop().show()

print("Drop row with null values having threshold")
df.na.drop(thresh=1).show()

print("Drop all row with null values")
df.na.drop(how='all').show()

print("Drop any row with null values")
df.na.drop(how='all').show()

print("Drop column(s) rows with null values")
df.na.drop(subset=['Sales']).show()

print("Fill column(s) rows with null values")
df.na.fill('No Name', subset=['Name']).show()

print("Fill column(s) rows with null values with Mean")
df.na.fill(df.select(mean(df['Sales'])).collect()[0][0], subset=['Sales']).show()
