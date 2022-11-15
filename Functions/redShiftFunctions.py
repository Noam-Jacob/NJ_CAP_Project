from pyspark.sql import DataFrame
import config

"""
Desc:
    redShiftWrite writes the dataframe provided to Redshift.

Arg:
    :DataFrame: The dataframe to be inserted into the database.
    :tableName: Name of the table.
""" 
def redShiftWrite(dataSetName:DataFrame, tableName:str):
    dataSetName.write \
    .format("jdbc") \
    .option("url", config.redshiftURL) \
    .option("dbtable", tableName) \
    .option("tempdir", config.redShiftTempFolder) \
    .option("user", config.userName) \
    .option("password", config.password) \
    .mode("append") \
    .save()

"""
Desc:
    redShiftRead reads a table provided to Redshift.

Arg:
    :spark: Provide the spark session.
    :tableName: Name of the table to be read from.
""" 
def redShiftRead(spark, tableName:str):
    df = spark.read \
    .format("jdbc") \
    .option("url", config.redshiftURL) \
    .option("dbtable", tableName) \
    .option("user", config.userName) \
    .option("password",  config.password) \
    .option("tempdir", config.redShiftTempFolder) \
    .load()
    return df

"""
Desc:
    redShiftQuery returns the results of a select query provided.

Arg:
    :spark: Provide the spark session.
    :query: Select query to be executed on the Redshift database.
""" 
def redShiftQuery(spark, query:str):
    df = spark.read \
    .format("jdbc") \
    .option("url", config.redshiftURL) \
    .option("query", query) \
    .option("user", config.userName) \
    .option("password",  config.password) \
    .load()
    return df