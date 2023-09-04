###################################################
## ETL Script for BNPL Industry Project Datasets ##
##          Author: James La Fontaine            ##
##           Last Edited: 03/09/2023             ##
###################################################

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
    SparkSession.builder.appName("ETL")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config('spark.driver.memory', '4g')
    .config('spark.executor.memory', '2g')
    .getOrCreate()
)



################################
## DOWNLOAD EXTERNAL DATASETS ##
################################

print("Downloading external datasets...")

# Define the output directory where you want to save the downloaded and extracted files
output_dir = 'data'

if not os.path.exists(output_dir):
    print("Could not find directory")
else:
    os.makedirs(output_dir + "/externaldataset", exist_ok=True)
    print("Download Start")
    
    # URL for the first dataset
    url1 = "https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_SA2_for_AUS_short-header.zip"
    output_file1 = output_dir + "/externaldataset/2021_GCP_SA2_for_AUS_short-header.zip"
    urlretrieve(url1, output_file1)
    
    # URL for the second dataset
    url2 = "https://data.gov.au/data/dataset/6cd8989d-4aca-46b7-b93e-77befcffa0b6/resource/cb659d81-5bd2-41f5-a3d0-67257c9a5893/download/asgs2021codingindexs.zip"
    output_file2 = output_dir + "/externaldataset/asgs2021codingindexs.zip"
    urlretrieve(url2, output_file2)
    
    # URL for the third dataset
    
    url3 = "https://www.abs.gov.au/statistics/people/population/regional-population/2021-22/32180DS0003_2001-22r.xlsx"
    output_file3 = output_dir + "/externaldataset/SA2_Populations_AUS.xlsx"
    urlretrieve(url3, output_file3)
    
    print("Download Complete")

# List of files to extract from the ZIP files
files_to_extract1 = [
    "2021Census_G02_AUST_SA2.csv",
    "2021Census_G33_AUST_SA2.csv",
    "2021Census_G17A_AUST_SA2.csv",
    "2021Census_G17B_AUST_SA2.csv",
    "2021Census_G17C_AUST_SA2.csv",
]

# file from the second dataset
files_to_extract2 = [
    "2022 Locality to 2021 SA2 Coding Index.csv"    
]

# Path to the ZIP files
zip_file_path1 = output_file1
zip_file_path2 = output_file2

# Destination folder for extraction
destination_folder = "data/externaldataset"

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

print("Extraction completed.")

######################
## LOAD IN DATASETS ##
######################

print("Loading in datasets...")

######################################## TRANSACTIONS ########################################

# 'transactions.parquet' files
path = 'data/tables/transactions_20210228_20210827_snapshot'
transactions_21_02_21_08 = spark.read.parquet(path)

path = 'data/tables/transactions_20210828_20220227_snapshot'
transactions_21_08_22_02 = spark.read.parquet(path)

path = 'data/tables/transactions_20220228_20220828_snapshot'
transactions_22_02_22_08 = spark.read.parquet(path)

######################################## CONSUMER ########################################
path = 'data/tables/consumer_fraud_probability.csv'
cust_fp = spark.read.csv(path, header=True)

path = 'data/tables/consumer_user_details.parquet'
cust_user_det = spark.read.parquet(path)

path = 'data/tables/tbl_consumer.csv'
cust_tbl = spark.read.csv(path, sep='|', header=True)

######################################## MERCHANT ########################################
path = 'data/tables/merchant_fraud_probability.csv'
merch_fp = spark.read.csv(path, header=True)

path = 'data/tables/tbl_merchants.parquet'
merch_tbl = spark.read.parquet(path)

######################################## EXTERNAL ########################################
# 2021 census data CSVs

path = 'data/externaldataset/2021 Census GCP Statistical Area 2 for AUS/*'
sa2_census = spark.read.csv(path, header=True)

#========================================================================================#

# SA2 population data

path = 'data/externaldataset/SA2_Populations_AUS.xlsx'

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

path = 'data/externaldataset/2022 Locality to 2021 SA2 Coding Index.csv'

sa2_to_postcode = spark.read.csv(path, header=True)

####################################################################
## REMOVE USELESS COLUMNS, DATA TYPE CONVERSIONS, COLUMN RENAMING ##
####################################################################

print("Removing useless columns, converting data types, renaming columns...")

######################################## TRANSACTIONS ########################################


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


################################################
## CLEANING, FEATURE ENGINEERING, AGGREGATION ##
################################################

print("Cleaning, feature engineering, aggregating...")

######################################## TRANSACTIONS ########################################

transactions_all = transactions_21_02_21_08.union(transactions_21_08_22_02).union(transactions_22_02_22_08)

df = transactions_all

# 1. Check for Null Values
df = df.na.drop()

# 2. Check for Duplicates
df = df.dropDuplicates()
df = df.dropDuplicates(['order_id'])

# 3. Validate user_id and merchant_abn
df = df.filter((F.col('user_id') > 0) & (F.col('merchant_abn') > 0))

# 4. Validate dollar value
df = df.filter(F.col('dollar_value') > 0)

# 5. Validate order_datetime for the specified date range
start_date = datetime.strptime("20210228", "%Y%m%d").date()
end_date = datetime.strptime("20221026", "%Y%m%d").date()
df = df.filter(F.col('order_datetime').between(start_date, end_date))

transactions_all_clean = df.drop('order_id')

######################################## CONSUMER ########################################

df = cust_user_det

# 1. Check for Null Values
df = df.na.drop()

# 2. Check for Duplicates
df = df.dropDuplicates()

# 3. Validate consumer_id and user_id
df = df.filter((F.col('consumer_id') > 0) & (F.col('user_id') > 0))

cust_user_det_clean = df

#==========================================================================================#

df = cust_tbl

# 1. Check for Null Values
df = df.na.drop()

# 2. Check for Duplicates
df = df.dropDuplicates()

# 3. Validate consumer_id
df = df.filter((F.col('consumer_id') > 0))

# 4. Validate consumer_name 
df = df.filter(F.col('consumer_name').rlike("[a-zA-Z][a-zA-Z ]+"))

# 5. Validate consumer_state 
df = df.filter(F.col('consumer_state').isin(['NT','ACT','SA','TAS','WA','QLD','VIC','NSW']))

# 6. Validate consumer_postcode
df = df.filter(F.col('consumer_postcode').between(200, 9729))

# 7. Validate consumer_gender
df = df.filter(F.col('consumer_gender').isin(['Undisclosed', 'Male', 'Female']))

cust_tbl_clean = df

#========================================================================================#

df = cust_fp

# 1. Check for Null Values
df = df.na.drop()

# 2. Check for Duplicates
df = df.dropDuplicates()

# 3. Validate user_id
df = df.filter((F.col('user_id') > 0))

# 4. Validate order_datetime for the specified date range
start_date = datetime.strptime("20210228", "%Y%m%d").date()
end_date = datetime.strptime("20221026", "%Y%m%d").date()
df = df.filter(F.col('order_datetime').between(start_date, end_date))

# 5. Validate consumer fraud probability
df = df.filter(F.col('consumer_fraud_probability_%').between(0, 100))

cust_fp_clean = df


######################################## MERCHANT ########################################

# Extract different features from tags (words, revenue band, take rate)
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType, ArrayType

# Define a UDF to extract the sections within square and normal brackets
def split_tags(input_tags):
    sep_tags = re.findall(r'[\[\(\{]([^\[\]\(\)\{\}]*)[\]\)\}]', input_tags)
    sep_tags_without_brackets = [re.sub(r'[\[\]\(\)\{\}]', '', tag) for tag in sep_tags]
        
    return sep_tags_without_brackets

# Define a UDF to extract "take rate" numbers
def extract_take_rate(input_rate):
    match = re.search(r'take rate: ([\d.]+)', input_rate)
    if match:
        return float(match.group(1))
    return None

# Register the UDFs
split_tags_udf = udf(split_tags, ArrayType(StringType()))

clean_take_rate_udf = udf(extract_take_rate, FloatType())

# Apply the UDF to the DataFrame
merch_tbl_clean = merch_tbl.withColumn("sep_tags", split_tags_udf(merch_tbl["tags"]))

# Create separate columns each segment of the tags
merch_tbl_clean = merch_tbl_clean.withColumn("words", merch_tbl_clean["sep_tags"].getItem(0))
merch_tbl_clean = merch_tbl_clean.withColumn("revenue_level", merch_tbl_clean["sep_tags"].getItem(1))
merch_tbl_clean = merch_tbl_clean.withColumn("take_rate_%", merch_tbl_clean["sep_tags"].getItem(2))

merch_tbl_clean = merch_tbl_clean.withColumn("take_rate_%", clean_take_rate_udf(merch_tbl_clean["take_rate_%"]))

merch_tbl_clean = merch_tbl_clean.drop('tags', 'sep_tags')

df = merch_tbl_clean 

# 1. Check for Null Values
df = df.na.drop()

# 2. Check for Duplicates
df = df.dropDuplicates()

# 3. Validate merchant_abn
df = df.filter((F.col('merchant_abn') > 0))

# 4. Validate merchant_name 
df = df.filter(F.col('merchant_name').rlike("[a-zA-Z][a-zA-Z ]+"))

# 5. Validate words (SPELL CHECK?) # https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html#sklearn.feature_extraction.text.TfidfVectorizer)
                                   # https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html 
#df = df.filter(F.col('words') in ['NT','ACT','SA','TAS','WA','QLD','VIC','NSW'])

# 6. Validate take_rate
df = df.filter((F.col('take_rate_%').between(0, 100)))

# 7. Validate revenue_level
df = df.filter(F.col('revenue_level').isin(['a', 'b', 'c', 'd', 'e']))


merch_tbl_clean = df

#========================================================================================#

df = merch_fp

# 1. Check for Null Values
df = df.na.drop()

# 2. Check for Duplicates
df = df.dropDuplicates()

# 3. Validate merchant_abn
df = df.filter((F.col('merchant_abn') > 0))

# 4. Validate order_datetime for the specified date range
start_date = datetime.strptime("20210228", "%Y%m%d").date()
end_date = datetime.strptime("20221026", "%Y%m%d").date()
df = df.filter(F.col('order_datetime').between(start_date, end_date))

# 5. Validate merchant fraud probability
df = df.filter(F.col('merchant_fraud_probability_%').between(0, 100))

merch_fp_clean = df

######################################## EXTERNAL ########################################

# 2021 census data CSVs

#========================================================================================#

# SA2 population data

df = sa2_pops

# 1. Check for Null Values
df = df.na.drop()

# 2. Check for Duplicates
df = df.dropDuplicates()

# 3. Validate population
df = df.filter(F.col('population_2021') >= 0)

sa2_pops_clean = df




#========================================================================================#

# Locality to SA2 coding index

df = sa2_to_postcode

# 1. Check for Null Values
df = df.na.drop()

# 2. Check for Duplicates
df = df.dropDuplicates()

# No need to validate postcode as inner join will be performed with validated postcodes

sa2_to_postcode_clean=df


######################################## AGGREGATION ########################################

print("Aggregating data...")

output_relative_dir = '../data/curated/'

# check if it exists as it makedir will raise an error if it does exist
if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)
   

# now, for each type of data set we will need, we will create the paths
for target_dir in ('consumer', 'merchant'): # taxi_zones should already exist
    if not os.path.exists(output_relative_dir + target_dir):
        os.makedirs(output_relative_dir + target_dir)


orig_combined = cust_tbl_clean.join(cust_user_det_clean, on='consumer_id', how='inner') \
.join(transactions_all_clean, on='user_id', how ='inner') \
.join(cust_fp_clean, on=['user_id', 'order_datetime'], how='left').na.fill(0) \
.join(merch_tbl_clean, on='merchant_abn', how='inner') \
.join(merch_fp_clean, on=['merchant_abn', 'order_datetime'], how='left').na.fill(0)

sa2_combined = sa2_to_postcode_clean.join(sa2_pops_clean, on='sa2_code', how='inner').withColumnRenamed('population_2021','sa2_population_2021')

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

print("ETL Script Complete.")