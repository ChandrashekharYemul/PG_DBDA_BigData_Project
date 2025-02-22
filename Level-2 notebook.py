# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

client_appli_id = "0f83e1be-1c0a-450f-bee7-58b1b2657af2"
tenent_dir_id = "ca1fbde3-fb37-4009-8f5c-df35603e1bde"
secret_id = "1C98Q~rRhjR6DXB01AkkgDPCLmz.CMs-cKSg3aFj"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.taxidatalakechan099.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.taxidatalakechan099.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.taxidatalakechan099.dfs.core.windows.net", "0f83e1be-1c0a-450f-bee7-58b1b2657af2")
spark.conf.set("fs.azure.account.oauth2.client.secret.taxidatalakechan099.dfs.core.windows.net", "1C98Q~rRhjR6DXB01AkkgDPCLmz.CMs-cKSg3aFj")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.taxidatalakechan099.dfs.core.windows.net", "https://login.microsoftonline.com/ca1fbde3-fb37-4009-8f5c-df35603e1bde/oauth2/token")

# COMMAND ----------

dbutils.fs.ls('abfss://level1@taxidatalakechan099.dfs.core.windows.net/')

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Read

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read csv data
# MAGIC ##### Trip type data

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load('abfss://level1@taxidatalakechan099.dfs.core.windows.net/trip_types')

# COMMAND ----------

df_trip_type.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Trip zone data

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load('abfss://level1@taxidatalakechan099.dfs.core.windows.net/trip_zones')

# COMMAND ----------

df_trip_zone.limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trip data 2023

# COMMAND ----------

myschema = '''
		        VendorID BIGINT,
		        lpep_pickup_datetime TIMESTAMP,
		        lpep_dropoff_datetime TIMESTAMP,
		        store_and_fwd_flag STRING,
		        RatecodeID BIGINT,         
		        PULocationID BIGINT,
		        DOLocationID BIGINT,
		        passenger_count BIGINT,
		        trip_distance DOUBLE,
		        fare_amount DOUBLE,
		        extra DOUBLE,
		        mta_tax DOUBLE,
		        tip_amount DOUBLE,
		        tolls_amount DOUBLE,
		        ehail_fee DOUBLE,
		        improvement_surcharge DOUBLE,
		        total_amount DOUBLE,
		        payment_type LONG,
		        trip_type BIGINT,
		        congestion_surcharge DOUBLE
          
'''

# COMMAND ----------

df_taxi_data = spark.read.format('parquet')\
                .schema(myschema)\
                .option('header',True)\
                .option('recursiveFileLookup',True)\
                .load('abfss://level1@taxidatalakechan099.dfs.core.windows.net/2023taxidata')

# COMMAND ----------

df_taxi_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data transformation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### write data into level2 container

# COMMAND ----------

df_trip_type.write.format('parquet')\
            .mode('append')\
            .option("path",'abfss://level2@taxidatalakechan099.dfs.core.windows.net/trip_types')\
            .save()

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('zone1',split(col('zone'),'/')[0])\
                            .withColumn('zone2',split(col('zone'),'/')[1]) 
df_trip_zone.limit(10).show()

# COMMAND ----------

df_trip_zone.write.format('parquet')\
            .mode('append')\
            .option("path",'abfss://level2@taxidatalakechan099.dfs.core.windows.net/trip_zones')\
            .save()

# COMMAND ----------

df_taxi_data = df_taxi_data.withColumn('trip_date',to_date('lpep_pickup_datetime'))\
                            .withColumn('trip_year',year('lpep_pickup_datetime'))\
                            .withColumn('trip_month',month('lpep_pickup_datetime'))

# COMMAND ----------

df_taxi_data.display()

# COMMAND ----------

df_taxi_data.printSchema()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# COMMAND ----------

df_source = spark.read.parquet("abfss://level1@taxidatalakechan099.dfs.core.windows.net/2023taxidata/trip-data/green_tripdata_2023-01.parquet")

# COMMAND ----------

df_source = df_source.withColumn("trip_distance", round(col("trip_distance")).cast("long")) \
    .withColumn("fare_amount", round(col("fare_amount")).cast("long")) \
    .withColumn("extra", round(col("extra")).cast("long")) \
    .withColumn("mta_tax", round(col("mta_tax")).cast("long")) \
    .withColumn("tip_amount", round(col("tip_amount")).cast("long")) \
    .withColumn("tolls_amount", round(col("tolls_amount")).cast("long")) \
    .withColumn("improvement_surcharge", round(col("improvement_surcharge")).cast("long")) \
    .withColumn("total_amount", round(col("total_amount")).cast("long")) \
    .withColumn("congestion_surcharge", round(col("congestion_surcharge")).cast("long"))

# COMMAND ----------

df_source = df_source.withColumn(
    "ehail_fee",
    when(col("ehail_fee").isNull(), lit(0))
    .otherwise(round(col("ehail_fee")))
    .cast("long")
)

# COMMAND ----------

df_source.write.mode("overwrite").parquet("abfss://level2@taxidatalakechan099.dfs.core.windows.net/new_taxi_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis

# COMMAND ----------

display(df_taxi_data)

# COMMAND ----------

