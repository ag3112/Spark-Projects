from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def read_data(spark_session: SparkSession, data_location: str, data_format: str, header: bool) -> DataFrame:
    if data_format.upper() == 'CSV':
        x = spark_session.read.format(data_format).load(data_location, header=header)
    else:
        x = spark_session.read.format(data_format).load(data_location)

    return x
