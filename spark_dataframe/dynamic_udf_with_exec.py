from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

from spark_session_utils import SparkSessionUtils

code_string = '''def convert_case(input_string):
        res_str = ""
        arr = input_string.split(" ")
        for x in arr:
            res_str = res_str + x[0:1].upper() + x[1:len(x)] + " "
        print(res_str)
        return res_str

output = convert_case(input_string)
'''


def do_run_script_python(code_str, input_data):
    method_input = {"input_string": input_data}
    exec(code_str, method_input)
    return method_input["output"]


if __name__ == '__main__':
    spark = SparkSessionUtils('dynamic method loader with exec').get_spark_session()
    columns = ["Seqno", "Name"]
    data = [("1", "john jones"),
            ("2", "tracey smith"),
            ("3", "amy sanders")]

    df = spark.createDataFrame(data=data, schema=columns)
    df.show(truncate=False)

    code_string = '''def convert_case(input_string):
            res_str = ""
            arr = input_string.split(" ")
            for x in arr:
                res_str = res_str + x[0:1].upper() + x[1:len(x)] + " "
            print(res_str)
            return res_str

output = convert_case(input_string)
    '''

    """ Converting function to UDF """

    convertUDF = udf(lambda z: do_run_script_python(code_str=code_string, input_data=str(z)))

    df.select(col("Seqno"), convertUDF(col("Name")).alias("Name")).show(truncate=False)
