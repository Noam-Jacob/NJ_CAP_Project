#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datetime, math 
import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, lower, upper, col, trim, year, month, dayofweek, translate, concat, regexp_replace, date_format, date_add
from pyspark.sql.types import StringType , IntegerType
from pyspark import SparkConf, SparkContext
from redShiftFunctions import redShiftQuery, redShiftWrite, redShiftRead

# REPLACE ACCESS KEY AND SECRET KEY in config.py file!
conf = SparkConf().set("spark.hadoop.fs.s3a.access.key", config.s3aAccessKey).set("spark.hadoop.fs.s3a.secret.key", config.s3aSecretKey).set("spark.hadoop.fs.s3a.endpoint", config.s3aEndpoint)

spark = SparkSession.builder.appName("Main ETL for the Capstone Project").config(conf=conf).getOrCreate()

#Used to collect all dataframes.
tableNamesArray = []


# In[7]:



#  ------  PORT LOCATION TABLE -----------

#Read the Portal Codes json file.
textFile = spark.sparkContext.textFile("Datasets/Immigration Data/prtlCodes.json")
dfPortCodes = spark.read.json(textFile)

#Remove any rows that do not contain a name and drop duplicates.
dfPortCodes = dfPortCodes.filter(~dfPortCodes.name.contains("No PORT") & ~dfPortCodes.name.contains("Collapsed") ).drop_duplicates()

#Split the name to columns municipality and statecode.
nameSplit = split(dfPortCodes.name, ',')
dfPortCodes = dfPortCodes.withColumn('municipality', trim (lower( nameSplit.getItem(0) ) ) ).withColumn('statecode', trim (upper( nameSplit.getItem(1) ) ) )

#Remove any rows without state code. Drop column name as its been split to two new columns.
dfPortCodes = dfPortCodes.filter(~dfPortCodes.statecode.isNull()).drop("name")

dfStateCode = spark.read.options(header=True, delimiter=',', inferSchema='True').csv("Datasets/States/State_Codes.csv")

dfPortCodes = dfPortCodes.join(dfStateCode, dfPortCodes.statecode == dfStateCode["Alpha code"], "left")
dfPortCodes = dfPortCodes.drop("Alpha code").withColumn("State", lower(dfPortCodes.State))

dfPortCodes.createOrReplaceTempView("PORTCODES")

#dfPortCodes.show()
tableNamesArray.clear()
tableNamesArray.append({'PORTCODES':dfPortCodes})


# In[3]:


#
#
#  ------  AIRPORTS TABLE
#
#

dfAirPorts = spark.read.option("header",True).csv("Datasets/Airport Code/airport-codes_csv.csv")

# Our analysis will be set for US only.
dfAirPorts = dfAirPorts.filter( (dfAirPorts.iso_country == 'US') & (dfAirPorts.type != 'heliport') & (dfAirPorts.type != 'closed') )
latLongSplit = split(dfAirPorts.coordinates, ',')
isoRegSplit = split(dfAirPorts.iso_region, '-')

dfAirPorts = dfAirPorts.withColumn('longitude', latLongSplit.getItem(0)).withColumn('latitude', latLongSplit.getItem(1)).withColumn('statecode', upper(isoRegSplit.getItem(1))).withColumn('municipality', lower(dfAirPorts.municipality)) 

#Remove columns continent and iso_country since it is US only.
dfAirPorts = dfAirPorts.drop("iso_region", "continent", "coordinates", "gps_code", "local_code")

dfAirPorts.createOrReplaceTempView("AIRPORTS_VIEW")

#Append table.
tableNamesArray.append({'AIRPORTS':dfAirPorts})


# In[8]:


#
#
#  ------  USATEMP  TABLE
#
#

dfTemps = spark.read.options(header=True, delimiter=',', inferSchema='True').csv("Datasets/Tempature/GlobalLandTemperaturesByState.csv")
dfTemps = dfTemps.filter( (dfTemps.Country == 'United States') & (dfTemps.AverageTemperature != 'Nan') )
dfTemps = dfTemps.withColumn('year', year(dfTemps.dt)).withColumn('month', month(dfTemps.dt)).withColumn('dayOfweek', dayofweek(dfTemps.dt)).withColumn('State', lower (dfTemps.State)) 
          
dfTemps = dfTemps.withColumn("id", concat(translate(dfTemps["dt"], "-" , ""), dfTemps.State ))
dfTemps = dfTemps.drop("dt", "Country")
dfTemps.createOrReplaceTempView("USATEMP")

#Append table.
tableNamesArray.append({'USATEMP':dfTemps})


# In[10]:


#
#
#  ------  USADEMOGRAPHICS  TABLE
#
#

dfCityDemo = spark.read.options(header=True, delimiter=';').csv("Datasets/US Demographics/us-cities-demographics.csv")
dfCityDemo = dfCityDemo.withColumn('City', lower(dfCityDemo.City)).withColumn('State', lower(dfCityDemo.State))

dfCityDemo = dfCityDemo.withColumnRenamed('City', 'city').withColumnRenamed('State', 'state').withColumnRenamed('Median Age', 'median_age').withColumnRenamed('Male Population', 'male_population').withColumnRenamed('Female Population', 'female_population').withColumnRenamed('Total Population', 'total_population').withColumnRenamed('Number of Veterans', 'number_of_veterans').withColumnRenamed('Foreign-born', 'foreign_born').withColumnRenamed('Average Household Size', 'average_household_size').withColumnRenamed('State Code', 'state_code').withColumnRenamed('Race', 'race').withColumnRenamed('Count', 'count') 

dfCityDemo.createOrReplaceTempView("USADEMOGRAPHICS")
dfCityDemo = spark.sql("SELECT * FROM USADEMOGRAPHICS WHERE (state_code, city) in (select statecode, municipality from PORTCODES)")

tableNamesArray.append({'USADEMOGRAPHICS':dfCityDemo})


# In[31]:


#
#
#  ------  IMMIGRATION  TABLE
#


dfImmigration = spark.read.options(inferSchema='True').parquet("Datasets/Immigration Data/*.snappy.parquet")


dfImmigration = dfImmigration.filter(dfImmigration["i94mode"] == 1)

dfImmigration = dfImmigration.withColumn("arrdate", regexp_replace(dfImmigration["arrdate"], '\..*$', '').cast(IntegerType()) ).withColumn("depdate", regexp_replace(dfImmigration["depdate"], '\..*$', '').cast(IntegerType()) )


dfImmigration.createOrReplaceTempView("dateTable")

dfImmigration = spark.sql("""
                    SELECT T1.*, 
                    date_add('1960-01-01', arrdate ) as arrival_full,
                    day(date_add('1960-01-01', arrdate ) ) as arrival_day,
                    month(date_add('1960-01-01', arrdate ) ) as arrival_month,
                    year(date_add('1960-01-01', arrdate ) ) as arrival_year,
                    day(date_add('1960-01-01', depdate) ) as dep_day,
                    month(date_add('1960-01-01', depdate) ) as dep_month,
                    year(date_add('1960-01-01', depdate) ) as dep_year
                    FROM dateTable T1 
                    """)

dfImmigration = dfImmigration.drop("depdate","i94mode","count", "admnum","entdepa","entdepd","entdepu","matflag","insnum")




dfImmigration.createOrReplaceTempView("dateTable1")



dfImmigration.limit(10).toPandas()
#df2.count()


# In[ ]:


#Writes all Dataframes into Redshift.
for tables in tableNamesArray:
    [[key, value]] = tables.items()
    redShiftWrite(value, key)


# In[24]:


#Confirms data has been inserted into the tables.
for tables in tableNamesArray:
    [[key, value]] = tables.items()
    currentTable = redShiftRead(spark, tables)
    if currentTable.count() = 0:
        print("Count incorrect on table" + tables)
    else:
        print("Count incorrect on table" + tables)


# In[8]:


lolframe = redShiftQuery(spark, "select trip_id, count(*) from trips group by trip_id having count(*) > 1" )
lolframe.toPandas()


# In[ ]:




