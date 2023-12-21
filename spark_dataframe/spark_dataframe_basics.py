from pyspark.sql.types import StructField, StringType, IntegerType, StructType

from spark_session_utils import SparkSessionUtils

# Create a SparkSession
spark = SparkSessionUtils("Spark Dataframe Basic").get_spark_session()

# Read a JSON file into a DataFrame
df_1 = spark.read.json("../data_source/people.json")

# Show the contents of the DataFrame
df_1.show()

# Print the schema of the DataFrame
df_1.printSchema()

# Print the columns of the DataFrame
print(df_1.columns)

# Compute basic statistics of the DataFrame
df_1.describe().show()

# Define the schema for the DataFrame
data_schema = [
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True)
]
final_structure = StructType(fields=data_schema)

# Read the same JSON file into a DataFrame with the defined schema
df_2 = spark.read.json("../data_source/people.json", schema=final_structure)

# Show the contents of the DataFrame
df_2.show()

# Print the schema of the DataFrame
df_2.printSchema()

# Print the columns of the DataFrame
print(df_2.columns)

# Compute basic statistics of the DataFrame
df_2.describe().show()

# Get a column from dataframe
df_selected_column_1 = df_2['name']
print(type(df_selected_column_1))

# Get a column from dataframe with dataframe type
df_selected_column_2 = df_2.select('name')
print(type(df_selected_column_2))

# Get a row from dataframe
print(df_2.head(1)[0])

# Create new column
df_2.withColumn("double_age", df_2['age'] * 2).show()

# Rename a column
df_2.withColumnRenamed("age", "New_Age").show()

# Register this dataframe
df_1.createOrReplaceTempView("people")

# Call the above dataframe using SQL
result = spark.sql('SELECT * FROM people WHERE age = 19')
result.show()

# Read a JSON file into a DataFrame
df_3 = spark.read.csv("../data_source/appl_stock.csv", inferSchema=True, header=True)
df_3.printSchema()
df_3.show()

# Use filter
df_3.filter("Close < 500").select(["Date", "Open"]).show()

# Multiple filters
df_3.filter((df_3["Close"] < 500) & (df_3["Open"] > 200)).show()

# Rows as dictionary
result_in_rows = df_3.filter((df_3["Close"] < 500) & (df_3["Open"] > 200)).collect()
row = result_in_rows[0]
print(row)
print(row.asDict()['Volume'])
