from importlib import import_module

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf


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
    spark = SparkSession.builder.appName('Spark').getOrCreate()
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

    # def upper_case(input_string):
    #     return input_string.upper()
    #
    #
    # upperCaseUDF = udf(lambda z: upper_case(z), StringType())
    #
    # df.withColumn("Cureated Name", upperCaseUDF(col("Name"))) \
    #     .show(truncate=False)
    #
    # """ Using UDF on SQL """
    # spark.udf.register("convertUDF", convert_case, StringType())
    # df.createOrReplaceTempView("NAME_TABLE")
    # spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE").show(truncate=False)
    #
    # spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE " + \
    #           "where Name is not null and convertUDF(Name) like '%John%'") \
    #     .show(truncate=False)
    #
    # """ null check """
    #
    # columns = ["Seqno", "Name"]
    # data = [("1", "john jones"),
    #         ("2", "tracey smith"),
    #         ("3", "amy sanders"),
    #         ('4', None)]
    #
    # df2 = spark.createDataFrame(data=data, schema=columns)
    # df2.show(truncate=False)
    # df2.createOrReplaceTempView("NAME_TABLE2")
    #
    # spark.udf.register("_nullsafeUDF", lambda str: convert_case(str) if not str is None else "", StringType())
    #
    # spark.sql("select _nullsafeUDF(Name) from NAME_TABLE2") \
    #     .show(truncate=False)
    #
    # spark.sql("select Seqno, _nullsafeUDF(Name) as Name from NAME_TABLE2 " + \
    #           " where Name is not null and _nullsafeUDF(Name) like '%John%'") \
    #     .show(truncate=False)
