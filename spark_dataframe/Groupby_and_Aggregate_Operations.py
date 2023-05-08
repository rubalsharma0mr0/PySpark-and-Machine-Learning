from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, format_number

spark = SparkSession.builder.appName("Groupby and Aggregate Operations").getOrCreate()

df = spark.read.csv('../data_source/sales_info.csv', inferSchema=True, header=True)
df.printSchema()
df.show()

print("GroupBy and count")
df.groupBy('Company').count().show()

print("GroupBy and Mean")
df.groupBy('Company').mean().show()

print("GroupBy and Max")
df.groupBy('Company').max().show()

print("GroupBy and Min")
df.groupBy('Company').min().show()

print("GroupBy and Average")
df.groupBy('Company').avg().show()

print("Aggregate Operations")
df.agg({"Sales": "sum"}).show()

print("Aggregate Operations and GroupBy Operations")
df.groupBy('Company').agg({"Sales": "max"}).show()

print("Aggregate Operations with Alise and Number format")
df.select(stddev('Sales').alias("Standard deviation")).select(format_number('Standard deviation', 3)).show()

print("Order By Ascending")
df.orderBy('Sales').show()

print("Order By Decending")
df.orderBy(df['Sales'].desc()).show()
