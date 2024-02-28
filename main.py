from dependencies.spark import get_spark_session
from processors.data_processor import process_data

if __name__ == '__main__':
    spark_session = get_spark_session('DEMO_APP')
    result_df = process_data(spark_session, 'tests/data/', None)
    result_df.show()
