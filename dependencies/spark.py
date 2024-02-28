import os

from pyspark.sql import SparkSession


def get_spark_session(name: str) -> SparkSession:
    if os.getenv('env').upper() == 'TEST':
        session = SparkSession.builder.appName(name).master('local[1]').getOrCreate()
    else:
        print('*'*50)
        session = SparkSession.builder.appName(name).getOrCreate()
    return session
