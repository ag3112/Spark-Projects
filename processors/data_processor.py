from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (lit)

from dependencies.utilities import read_data


def process_data(spark_session: SparkSession, input_location: str, output_location: str) -> DataFrame:
    try:
        data_frame = read_data(spark_session, input_location, data_format='csv', header=True)
        data_frame = data_frame.withColumn('col', lit('const'))
    except Exception as e:
        print('Exception', e)
        raise Exception('Something wrong !')
    return data_frame
