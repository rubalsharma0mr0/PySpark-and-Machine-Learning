from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

# Create a SparkSession
spark = SparkSession.builder.appName("Test Spark DataFrame").getOrCreate()

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
