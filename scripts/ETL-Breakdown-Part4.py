###################################################
## ETL Script for BNPL Industry Project Datasets ##
##              Breakdown Part 2                 ##
##          Author: James La Fontaine            ##
##          Edited by: Dulan Wijeratne           ##
##           Last Edited: 07/09/2023             ##
###################################################

import openpyxl
import pandas as pd
import os
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
from urllib.request import urlretrieve
import zipfile

print("Creating spark session...")
spark = (
    SparkSession.builder.appName("Preprocessing_Yellow")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config('spark.driver.memory', '3g')   
    .config('spark.executor.memory', '4g')  
    .config('spark.executor.instances', '2')  
    .config('spark.executor.cores', '2')
    .getOrCreate()
)

################################################
##              READING IN DATA               ##
################################################
print("Reading in Data...")

orig_combined = spark.read.\
    parquet("data/checkpoint/checkpoint3/orig_combined.parquet")
sa2_combined = spark.read.\
    parquet("data/checkpoint/checkpoint3/sa2_combined.parquet")

print("Reading in Data Complete")

print("Joining Data")
all_combined = orig_combined.join(sa2_combined.withColumnRenamed('postcode','consumer_postcode'), on='consumer_postcode', how='inner')

all_combined = all_combined.withColumnRenamed(
    'sa2_code',
    'consumer_sa2_code'
).withColumnRenamed(
    'sa4_name',
    'consumer_sa4_name'
).withColumnRenamed(
    'sa3_name',
    'consumer_sa3_name'
).withColumnRenamed(
    'sa2_name',
    'consumer_sa2_name'
).withColumnRenamed(
    'sa2_population_2021',
    'consumer_sa2_pop_2021'
).withColumnRenamed(
    'words',
    'merchant_description'
).withColumnRenamed(
    'revenue_level',
    'merchant_revenue_level'
).withColumnRenamed(
    'take_rate_%',
    'merchant_take_rate_%'
)

print("Saving to data/curated/...")

all_combined.write.mode('overwrite').parquet("data/curated/all_data_combined.parquet")

print("Save completed")