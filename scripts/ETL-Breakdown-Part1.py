###################################################
## ETL Script for BNPL Industry Project Datasets ##
##              Breakdown Part 1                 ##
##          Author: James La Fontaine            ##
##          Edited by: Dulan Wijeratne           ##
##           Last Edited: 14/09/2023             ##
###################################################

import io
import requests
import openpyxl
import pandas as pd
import os
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
from urllib.request import urlretrieve
import zipfile

print("Running ETL Script...")

print("Creating spark session...")

# Create a spark session (which will run spark jobs)
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

output_relative_dir = '../data/curated/'

if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)


output_relative_dir = '../data/nulls&missing_analysis/'

if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)
    
# now, for each type of data set we will need, we will create the paths
for target_dir in ('consumer', 'merchant', 'transaction', 'sa2'): 
    if not os.path.exists(output_relative_dir + target_dir):
        os.makedirs(output_relative_dir + target_dir)


################################
## DOWNLOAD EXTERNAL DATASETS ##############################################################
################################



print("Downloading external datasets...")

# Define the output directory where you want to save the downloaded and extracted files
output_relative_dir = 'data/raw/'

if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)
    
if not os.path.exists(output_relative_dir + "externaldataset"):
    os.makedirs(output_relative_dir + "externaldataset", exist_ok=True)
        
    
print("Download Start")
    
# URL for the first dataset
url1 = "https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_SA2_for_AUS_short-header.zip"
output_file1 = output_relative_dir + "externaldataset/2021_GCP_SA2_for_AUS_short-header.zip"
urlretrieve(url1, output_file1)
    
# URL for the second dataset
url2 = "https://data.gov.au/data/dataset/6cd8989d-4aca-46b7-b93e-77befcffa0b6/resource/cb659d81-5bd2-41f5-a3d0-67257c9a5893/download/asgs2021codingindexs.zip"
output_file2 = output_relative_dir + "externaldataset/asgs2021codingindexs.zip"
urlretrieve(url2, output_file2)
    
# URL for the third dataset
    
url3 = "https://www.abs.gov.au/statistics/people/population/regional-population/2021-22/32180DS0003_2001-22r.xlsx"
output_file3 = output_relative_dir + "externaldataset/SA2_Populations_AUS.xlsx"
urlretrieve(url3, output_file3)
    
print("Download complete.")

# List of files to extract from the ZIP files
files_to_extract1 = [
    "2021Census_G02_AUST_SA2.csv",
]

# file from the second dataset
files_to_extract2 = [
    "2022 Locality to 2021 SA2 Coding Index.csv"    
]

# Path to the ZIP files
zip_file_path1 = output_file1
zip_file_path2 = output_file2

# Destination folder for extraction
destination_folder = "data/raw/externaldataset"

# Extract files from the first ZIP file
with zipfile.ZipFile(zip_file_path1, 'r') as zip_ref:
    for file_name in files_to_extract1:
        try:
            zip_ref.extract(f"2021 Census GCP Statistical Area 2 for AUS/{file_name}", destination_folder)
            print(f"Extracted from ZIP 1: {file_name}")
        except KeyError:
            print(f"File not found in ZIP 1: {file_name}")

# Extract files from the second ZIP file
with zipfile.ZipFile(zip_file_path2, 'r') as zip_ref:
    for file_name in files_to_extract2:
        try:
            zip_ref.extract(file_name, destination_folder)
            print(f"Extracted from ZIP 2: {file_name}")
        except KeyError:
            print(f"File not found in ZIP 2: {file_name}")
            
# URL where the zipfile of the shapefiles are stored on the ABS website
zip_file_url = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/POA_2021_AUST_GDA94_SHP.zip"
r = requests.get(zip_file_url)
z = zipfile.ZipFile(io.BytesIO(r.content))

# output directory, making sure it exists
dir = "data/raw/externaldataset/postcode_shapefiles"
os.makedirs(dir)

# writing data into directory
z.extractall(dir)
    
print("Extraction complete.")
  


######################
## LOAD IN DATASETS ##########################################################################
######################

print("Loading in datasets...")

######################################## CONSUMER ########################################
path = 'data/raw/tables/consumer_fraud_probability.csv'
cust_fp = spark.read.csv(path, header=True)

path = 'data/raw/tables/consumer_user_details.parquet'
cust_user_det = spark.read.parquet(path)

path = 'data/raw/tables/tbl_consumer.csv'
cust_tbl = spark.read.csv(path, sep='|', header=True)

######################################## MERCHANT ########################################
path = 'data/raw/tables/merchant_fraud_probability.csv'
merch_fp = spark.read.csv(path, header=True)

path = 'data/raw/tables/tbl_merchants.parquet'
merch_tbl = spark.read.parquet(path)

######################################## EXTERNAL ########################################
# 2021 census data CSVs

path = 'data/raw/externaldataset/2021 Census GCP Statistical Area 2 for AUS/2021Census_G02_AUST_SA2.csv'
sa2_census = spark.read.csv(path, header=True)

#========================================================================================#

# SA2 population data

path = 'data/raw/externaldataset/SA2_Populations_AUS.xlsx'

# Have to manually fix the dataframe as the formatting is messed up

sa2_pops = pd.read_excel(path, sheet_name=1)

# remove empty / useless rows at top
sa2_pops = sa2_pops.iloc[5:,:]

# drop unnecessary columns
sa2_pops = sa2_pops.drop(sa2_pops.iloc[:, 10:30],axis = 1)

sa2_pops = sa2_pops.drop(sa2_pops.columns[[0,1,2,3,4,6,11]], axis=1)

# fix header
sa2_pops.columns = sa2_pops.iloc[1]

sa2_pops = sa2_pops[3:]

# drop empty rows at bottom of table
sa2_pops = sa2_pops.dropna()

sa2_pops = sa2_pops.rename(columns={'no.': 'population_2021'})

#========================================================================================#

# Locality to SA2 coding index CSV

path = 'data/raw/externaldataset/2022 Locality to 2021 SA2 Coding Index.csv'

sa2_to_postcode = spark.read.csv(path, header=True)

####################################################################
## REMOVE USELESS COLUMNS, DATA TYPE CONVERSIONS, COLUMN RENAMING ############################
####################################################################

print("Removing useless columns, converting data types, renaming columns...")
######################################## CONSUMER ########################################
cust_fp = \
cust_fp.withColumn(
    'user_id',
    F.col('user_id').cast('long')
).withColumn(
    'order_datetime',
    F.col('order_datetime').cast('date')
).withColumn(
    'consumer_fraud_probability_%',
    F.col('fraud_probability').cast('double')
).drop('fraud_probability')

cust_tbl = \
cust_tbl.withColumn(
    'postcode',
    F.col('postcode').cast('long')
).withColumnRenamed(
    'name',
    'consumer_name'   
).withColumnRenamed(
    'state',
    'consumer_state'   
).withColumnRenamed(
    'postcode',
    'consumer_postcode'   
).withColumnRenamed(
    'gender',
    'consumer_gender'   
).withColumn(
    'consumer_id',
    F.col('consumer_id').cast('long')
).drop('address')


### MERCHANT ###
merch_fp = \
merch_fp.withColumn(
    'merchant_abn',
    F.col('merchant_abn').cast('long')
).withColumn(
    'order_datetime',
    F.col('order_datetime').cast('date')
).withColumn(
    'merchant_fraud_probability_%',
    F.col('fraud_probability').cast('double')
).drop('fraud_probability')

merch_tbl = \
merch_tbl.withColumnRenamed(
    'name',
    'merchant_name'
)


######################################## EXTERNAL ########################################

# 2021 census data

sa2_census = sa2_census.withColumn(
    'sa2_code',
    F.col('SA2_CODE_2021').cast('long')
).withColumn(
    'sa2_median_age',
    F.col('Median_age_persons').cast('long')
).withColumn(
    'sa2_median_mortgage_repay_monthly',
    F.col('Median_mortgage_repay_monthly').cast('long')
).withColumn(
    'sa2_median_tot_prsnl_inc_weekly',
    F.col('Median_tot_prsnl_inc_weekly').cast('long')
).withColumn(
    'sa2_median_rent_weekly',
    F.col('Median_rent_weekly').cast('long')
).withColumn(
    'sa2_median_tot_fam_inc_weekly',
    F.col('Median_tot_fam_inc_weekly').cast('long')
).withColumn(
    'sa2_average_num_psns_per_bedroom',
    F.col('Average_num_psns_per_bedroom').cast('double')
).withColumn(
    'sa2_median_tot_hhd_inc_weekly',
    F.col('Median_tot_hhd_inc_weekly').cast('long')
).withColumn(
    'sa2_average_household_size',
    F.col('Average_household_size').cast('double')
)

sa2_census = sa2_census.select(sa2_census.columns[9:])

#========================================================================================#

# SA2 population data
sa2_pops=spark.createDataFrame(sa2_pops)

# convert casings and rename columns where necessary
sa2_pops = \
sa2_pops.withColumnRenamed(
    'SA4 name',
    'sa4_name'
).withColumnRenamed(
    'SA3 name',
    'sa3_name'
).withColumnRenamed(
    'SA2 code',
    'sa2_code'
).withColumnRenamed(
    'SA2 name',
    'sa2_name'
)


#========================================================================================#

# Locality to SA2 coding index

sa2_to_postcode = sa2_to_postcode.select(['POSTCODE', 'SA2_MAINCODE_2021']).withColumn(
    'POSTCODE',
    F.col('POSTCODE').cast('long')
).withColumn(
    'SA2_MAINCODE_2021',
    F.col('SA2_MAINCODE_2021').cast('long')
).withColumnRenamed(
    'SA2_MAINCODE_2021',
    'sa2_code'
)

sa2_to_postcode = sa2_to_postcode.toDF(*[c.lower() for c in sa2_to_postcode.columns])

#===========================================================================================#

#Saving to checkpoint 1
print("Saving to checkpoint 1")

if not os.path.exists("data/checkpoint"):
    os.makedirs("data/checkpoint")

if not os.path.exists("data/checkpoint/checkpoint1"):
    os.makedirs("data/checkpoint/checkpoint1")


cust_fp.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint1/cust_fp.parquet")
cust_tbl.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint1/cust_tbl.parquet")
merch_fp.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint1/merch_fp.parquet")
merch_tbl.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint1/merch_tbl.parquet")
sa2_census.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint1/sa2_census.parquet")
sa2_pops.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint1/sa2_pops.parquet")
sa2_to_postcode.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint1/sa2_to_postcode.parquet")

print("Complete save to checkpoint1")
spark.stop()
#==========================================================================================#