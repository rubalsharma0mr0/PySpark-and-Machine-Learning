from pyspark.shell import spark


def read_csv_data(filepath):
    """
    Read a CSV file into a Spark dataframe.

    Args:
        filepath (str): The path to the CSV file.

    Returns:
        A Spark dataframe containing the CSV data.
    """
    # create a Spark dataframe by reading the CSV file using SparkSession
    df = spark.read.option('header', 'true').csv(filepath)

    # show the first 20 rows of the dataframe in the console
    df.show()

    # return the dataframe
    return df


# call the read_csv_data function with the path to the CSV file
df = read_csv_data('data_source/mydata.csv')
