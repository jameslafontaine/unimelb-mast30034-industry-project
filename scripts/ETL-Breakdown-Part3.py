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


##################################### TRANSACTIONS ########################################
transactions_all_clean = spark.read.\
    parquet("data/checkpoint/checkpoint2/transactions_all_clean.parquet")

######################################## CONSUMER ########################################
cust_fp_clean =spark.read.\
    parquet("data/checkpoint/checkpoint2/cust_fp_clean.parquet")
cust_tbl_clean =spark.read.\
    parquet("data/checkpoint/checkpoint2/cust_tbl_clean.parquet")
cust_user_det_clean = spark.read.\
    parquet("data/checkpoint/checkpoint2/cust_user_det_clean.parquet")

######################################## MERCHANT ########################################
merch_fp_clean = spark.read.\
    parquet("data/checkpoint/checkpoint2/merch_fp_clean.parquet")
merch_tbl_clean = spark.read.\
    parquet("data/checkpoint/checkpoint2/merch_tbl_clean.parquet")

######################################## EXTERNAL ########################################
sa2_census_clean = spark.read.\
    parquet("data/checkpoint/checkpoint2/sa2_census_clean.parquet")
sa2_pops_clean = spark.read.\
    parquet("data/checkpoint/checkpoint2/sa2_pops_clean.parquet")
sa2_to_postcode_clean = spark.read.\
    parquet("data/checkpoint/checkpoint2/sa2_tp_postcode_clean.parquet")
sa2_census = spark.read.\
    parquet("data/checkpoint/checkpoint1/sa2_census.parquet")
print("Reading in Data Complete")

######################################## AGGREGATION ########################################

print("Aggregating data...")

orig_combined = cust_tbl_clean.join(cust_user_det_clean, on='consumer_id', how='inner') \
.join(transactions_all_clean, on='user_id', how ='inner') \
.join(cust_fp_clean, on=['user_id', 'order_datetime'], how='left').na.fill(0) \
.join(merch_tbl_clean, on='merchant_abn', how='inner') \
.join(merch_fp_clean, on=['merchant_abn', 'order_datetime'], how='left').na.fill(0)


# add consumer to front of census stats before joining with consumer data
new_column_name_list= ['sa2_code'] + ['consumer_' + col for col in sa2_census_clean.columns[1:]]

sa2_census_clean = sa2_census_clean.toDF(*new_column_name_list)

sa2_combined = sa2_to_postcode_clean.join(sa2_pops_clean, on='sa2_code', how='inner').\
    withColumnRenamed('population_2021','sa2_population').\
    join(sa2_census, on='sa2_code', how='inner')


    
sa2_combined = sa2_to_postcode_clean.join(sa2_pops_clean, on='sa2_code', how='inner').\
    withColumnRenamed('population_2021','sa2_population').\
    join(sa2_census, on='sa2_code', how='inner')

##############################################################################################

#Saving to Checkpoint 3

#Create directory for checkpoint 3
if not os.path.exists("data/checkpoint/checkpoint3"):
    os.makedirs("data/checkpoint/checkpoint3")

print("Saving Data to checkpoint 3")
orig_combined.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint3/orig_combined.parquet")
sa2_combined.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint3/sa2_combined.parquet")

print("Completed save to checkpoint 3")

