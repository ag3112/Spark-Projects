import pytest

from processors.data_processor import process_data


def test_process_data(spark_fixture):
    result = process_data(spark_fixture, 'tests/data/', '')
    assert 'col' in result.columns


def test_process_data_1(spark_fixture):
    with pytest.raises(Exception) as exec_info:
        process_data(spark_fixture, 'tests/dat/', '')
    assert str(exec_info.value) == 'Something wrong !'


def test_process_data_1(spark_fixture):
    with pytest.raises(Exception) as exec_info:
        process_data(spark_fixture, 'tests/dat/', '')
    assert str(exec_info.value) == 'Something wrong !'


def test_mock(spark_fixture, mocker):
    mocker.patch('processors.data_processor.read_data', return_value=spark_fixture.createDataFrame([[1]], ['a']))
    result = process_data(None, 'tests/data/', '')
    assert 'col' in result.columns
    assert result.count() == 1
