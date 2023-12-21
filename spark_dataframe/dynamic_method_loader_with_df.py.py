from spark_session_utils import SparkSessionUtils


def do_run_script_python(code_string, input_data):
    method_input = {"dataframe": input_data}
    exec(code_string, method_input)
    return method_input["output"]


if __name__ == '__main__':
    spark = SparkSessionUtils('Dynamic Method Loader').get_spark_session()
    columns = ["Seqno", "Name"]
    data = [("1", "john jones"),
            ("2", "tracey smith"),
            ("3", "amy sanders")]

    df = spark.createDataFrame(data=data, schema=columns)
    df.show(truncate=False)

    code_str = '''def user_method_1(dataframe):
    def convert_case(input_string):
        res_str = ""
        arr = input_string.split(" ")
        for x in arr:
            res_str = res_str + x[0:1].upper() + x[1:len(x)] + " "
        return res_str
    from pyspark.sql.functions import col, udf
    convert_udf = udf(lambda z: convert_case(input_string=str(z)));

    dataframe = dataframe.select(col("Seqno"), convert_udf(col("Name")).alias("Name"))
    return dataframe'''

    code_str = code_str + "\noutput = user_method_1(dataframe)"

    df = do_run_script_python(code_str, df)
    df.show()
