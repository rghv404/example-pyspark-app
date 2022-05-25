from unittest import result
import pytest
from test_util.read_util import get_df_from_file
from src.oneAgent.main import sample_transform



@pytest.mark.usefixtures("spark_session")
def test_df_transform(spark_session):
    data = [["test1", 2], ["test2", 1], ["test3", 1]]
    cols = ["name", "cnt"]
    expected_df = spark_session.createDataFrame(data, cols)
    src_df = get_df_from_file(spark_session, path='./tests/resources/test.csv', format='csv', option="header")
    result_df = sample_transform(src_df)
    assert result_df.count() == expected_df.count()
