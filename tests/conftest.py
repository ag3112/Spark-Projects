import pytest

from dependencies.spark import get_spark_session


@pytest.fixture(scope='session')
def spark_fixture():
    session = get_spark_session('TEST-DEMO-APP')
    yield session
    session.stop()
