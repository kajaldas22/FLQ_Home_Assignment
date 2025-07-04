'''
Unit test for transform.py
check number of rows in transformed DataFrame
parittin column extraction
# and aggregation of debit and credit

'''

import pytest 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode



@pytest.fixture(scope="module")

def spark():
    return SparkSession.builder \
        .appName("testSession") \
        .getOrCreate()
     

def test_flatten_journal_lines(spark):
    data= [
        {"entry_id": "JE1", "lines": [
            {"account_id": "100", "debit": 500, "credit": 0},
            {"account_id": "200", "debit": 0, "credit": 500}
        ]}
    ]

    df=spark.createDataFrame(data)
    df_flatten=df.withColumn("line", explode(col("lines")))
    assert df_flatten.count()==2
    assert "line" in df_flatten.columns


def test_close_account_schema(spark):
    data=[
        { "account_id": "1000", "name": "Cash", "type": "Asset" },
        { "account_id": "2000", "name": "Accounts Payable", "type": "Liability" }
        ]
    
    df=spark.createDataFrame(data)
    assert "account"in df.columns
