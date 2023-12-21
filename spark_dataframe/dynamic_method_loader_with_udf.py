from importlib import import_module

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

from spark_session_utils import SparkSessionUtils


def dynamic_method_loader(module_name, method_name):
    try:
        module1 = import_module(module_name)

        if hasattr(module1, method_name):
            method_called = getattr(module1, method_name)
            return method_called
        else:
            print("Method not found.")
    except ImportError as e:
        print(f"Could not load with exception: {e}")


if __name__ == '__main__':
    spark = SparkSessionUtils('dynamic method loader with udf').get_spark_session()
    columns = ["Seqno", "Name"]
    data = [("1", "john jones"),
            ("2", "tracey smith"),
            ("3", "amy sanders")]

    df = spark.createDataFrame(data=data, schema=columns)
    df.show(truncate=False)

    # Open a file for writing (create a new file if it doesn't exist)
    file = open("dynamic1.py", "w")

    # Write some text to the file
    file.write('''def convert_case(input_string):
    res_str = ""
    arr = input_string.split(" ")
    for x in arr:
        res_str = res_str + x[0:1].upper() + x[1:len(x)] + " "
    return res_str''')

    # Close the file when done writing
    file.close()
    method_to_call = dynamic_method_loader(module_name='dynamic1', method_name='convert_case')
    """ Converting function to UDF """

    convertUDF = udf(lambda z: method_to_call(z))

    df.select(col("Seqno"), convertUDF(col("Name")).alias("Name")).show(truncate=False)
