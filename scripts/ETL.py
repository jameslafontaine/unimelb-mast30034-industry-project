###################################################
## ETL Script for BNPL Industry Project Datasets ##
##          Author: James La Fontaine            ##
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
    SparkSession.builder.appName("ETL")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config('spark.driver.memory', '4g')
    .config('spark.executor.memory', '2g')
    .getOrCreate()
)

output_relative_dir = 'data/curated/'

if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)
    
# now, for each type of data set we will need, we will create the paths
for target_dir in ('consumer', 'merchant', 'transaction', 'sa2'): 
    if not os.path.exists(output_relative_dir + target_dir):
        os.makedirs(output_relative_dir + target_dir)


output_relative_dir = 'data/nulls&missing_analysis/'

if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)
    
# now, for each type of data set we will need, we will create the paths
for target_dir in ('consumer', 'merchant', 'transaction', 'sa2'): 
    if not os.path.exists(output_relative_dir + target_dir):
        os.makedirs(output_relative_dir + target_dir)


################################
## DOWNLOAD EXTERNAL DATASETS ################################################################
################################
#region Download External Datasets

print("Downloading external datasets...")

# Define the output directory where you want to save the downloaded and extracted files
output_relative_dir = 'data/raw/'

if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)
    
if not os.path.exists(output_relative_dir + "externaldataset"):
    os.makedirs(output_relative_dir + "externaldataset", exist_ok=True)
    
if not os.path.exists(output_relative_dir + "externaldataset/postcode_shapefiles"):
    os.makedirs(output_relative_dir + "externaldataset/postcode_shapefiles", exist_ok=True)
        
    
print("Download start")
    
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

dir = "data/raw/externaldataset/postcode_shapefiles"

print("Downloading and extracting shapefiles...")

# writing data into directory
z.extractall(dir)
    
print("Extraction complete.")
  
# endregion

######################
## LOAD IN DATASETS ##########################################################################
######################
#region Load in Datasets
print("Loading in datasets...")

######################################## TRANSACTIONS ########################################

# 'transactions.parquet' files
path = 'data/raw/tables/transactions_20210228_20210827_snapshot'
transactions_21_02_21_08 = spark.read.parquet(path)

path = 'data/raw/tables/transactions_20210828_20220227_snapshot'
transactions_21_08_22_02 = spark.read.parquet(path)

path = 'data/raw/tables/transactions_20220228_20220828_snapshot'
transactions_22_02_22_08 = spark.read.parquet(path)

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

#endregion

####################################################################
## REMOVE USELESS COLUMNS, DATA TYPE CONVERSIONS, COLUMN RENAMING ############################
####################################################################
#region Preprocessing Step 1
print("Removing useless columns, converting data types, renaming columns...")

######################################## TRANSACTIONS ########################################
#region Transactions

#endregion
######################################## CONSUMER ############################################
#region Consumer

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

#endregion
######################################## EXTERNAL ########################################

#region External

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

#endregion External

#endregion Preprocessing Step 1

################################################
## CLEANING, FEATURE ENGINEERING, AGGREGATION ################################################
################################################
#region Preprocessing Step 2

print("Cleaning, feature engineering, aggregating...")

######################################## TRANSACTIONS ########################################

transactions_all = transactions_21_02_21_08.union(transactions_21_08_22_02).union(transactions_22_02_22_08)

df = transactions_all

df.write.mode('overwrite').parquet("data/nulls&missing_analysis/transaction/transactions_all.parquet")

# 1. Remove Null Values
df = df.na.drop()

# 2. Remove Duplicates
df = df.dropDuplicates()
df = df.dropDuplicates(['order_id'])

# 3. Validate user_id and merchant_abn
df = df.filter((F.col('user_id') > 0) & (F.col('merchant_abn') > 0))

# 4. Validate dollar value
df = df.filter(F.col('dollar_value') > 0)
df = df.withColumn('dollar_value', F.round(F.col('dollar_value'), 2))

# 5. Validate order_datetime for the specified date range
start_date = datetime.strptime("20210228", "%Y%m%d").date()
end_date = datetime.strptime("20221026", "%Y%m%d").date()
df = df.filter(F.col('order_datetime').between(start_date, end_date))


transactions_all_clean = df.drop('order_id')

######################################## CONSUMER ########################################

df = cust_user_det

df.write.mode('overwrite').parquet("data/nulls&missing_analysis/consumer/consumer_user_details.parquet")

# 1. Remove Null Values
df = df.na.drop()

# 2. Remove Duplicates
df = df.dropDuplicates()

# 3. Validate consumer_id and user_id
df = df.filter((F.col('consumer_id') > 0) & (F.col('user_id') > 0))

cust_user_det_clean = df

#==========================================================================================#

df = cust_tbl

df.write.mode('overwrite').parquet("data/nulls&missing_analysis/consumer/consumer_tbl.parquet")

# 1. Remove Null Values
df = df.na.drop()

# 2. Remove Duplicates
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

df.write.mode('overwrite').parquet("data/nulls&missing_analysis/consumer/consumer_fraud_probability.parquet")

# 1. Remove Null Values
df = df.na.drop()

# 2. Remove Duplicates
df = df.dropDuplicates()

# 3. Validate user_id
df = df.filter((F.col('user_id') > 0))

# 4. Validate order_datetime for the specified date range
start_date = datetime.strptime("20210228", "%Y%m%d").date()
end_date = datetime.strptime("20221026", "%Y%m%d").date()
df = df.filter(F.col('order_datetime').between(start_date, end_date))

# 5. Validate consumer fraud probability
df = df.filter(F.col('consumer_fraud_probability_%').between(0, 100))
df = df.withColumn('consumer_fraud_probability_%', F.round(F.col('consumer_fraud_probability_%'), 4))

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

# Replace multiple spaces with a single space
merch_tbl_clean = merch_tbl_clean.withColumn("words", F.regexp_replace(F.col("words"), "\\s+", " "))

# Trim leading and trailing spaces
merch_tbl_clean = merch_tbl_clean.withColumn("words", F.regexp_replace(F.col("words"), "^\\s+", ""))
merch_tbl_clean = merch_tbl_clean.withColumn("words", F.regexp_replace(F.col("words"), "\\s+$", ""))

# Ensure consistent casing
merch_tbl_clean = merch_tbl_clean.withColumn('words', F.lower(F.col('words')))

df = merch_tbl_clean 

df.write.mode('overwrite').parquet("data/nulls&missing_analysis/merchant/merchant_tbl.parquet")

# 1. Remove Null Values
df = df.na.drop()

# 2. Remove Duplicates
df = df.dropDuplicates()

# 3. Validate merchant_abn
df = df.filter((F.col('merchant_abn') > 0))

# 4. Validate merchant_name 
df = df.filter(F.col('merchant_name').rlike("[a-zA-Z][a-zA-Z ]+"))

# 5. Validate words (SPELL CHECK?) # https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html#sklearn.feature_extraction.text.TfidfVectorizer)
                                   # https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html 
#df = df.filter(F.col('words').rlike("[a-zA-Z][a-zA-Z0-9 ]+"))

# 6. Validate take_rate
df = df.filter((F.col('take_rate_%').between(0, 100)))

# 7. Validate revenue_level
df = df.filter(F.col('revenue_level').isin(['a', 'b', 'c', 'd', 'e']))


merch_tbl_clean = df

from pyspark.sql.types import StringType

# Create the segment feature for each merchant

tech_and_electronics = ["computer", "digital", "television", "telecom"]
retail_and_novelty = ["newspaper", "novelty", "hobby", "shoe", "instruments", "bicycle", "craft","office"]
garden_and_furnishings = ["florists", "furniture", "garden", "tent"]
antiques_and_jewellery = ["galleries", "antique", "jewelry"]
specialized_services = ["health", "motor", "opticians"]


def segment(description):
    for segment, keywords in [("tech_and_electronics", tech_and_electronics),
                               ("retail_and_novelty", retail_and_novelty),
                               ("garden_and_furnishings", garden_and_furnishings),
                               ("antiques_and_jewellery", antiques_and_jewellery),
                               ("specialized_services", specialized_services)]:
        if any(keyword in description for keyword in keywords):
            return segment
    return "other"

segment_udf = F.udf(segment, StringType())

merch_tbl_clean = merch_tbl_clean.withColumn("segment", segment_udf(merch_tbl_clean.words))

#========================================================================================#

df = merch_fp

df.write.mode('overwrite').parquet("data/nulls&missing_analysis/merchant/merchant_fraud_probability.parquet")

# 1. Remove Null Values
df = df.na.drop()

# 2. Remove Duplicates
df = df.dropDuplicates()

# 3. Validate merchant_abn
df = df.filter((F.col('merchant_abn') > 0))

# 4. Validate order_datetime for the specified date range
start_date = datetime.strptime("20210228", "%Y%m%d").date()
end_date = datetime.strptime("20221026", "%Y%m%d").date()
df = df.filter(F.col('order_datetime').between(start_date, end_date))

# 5. Validate merchant fraud probability
df = df.filter(F.col('merchant_fraud_probability_%').between(0, 100))
df = df.withColumn('merchant_fraud_probability_%', F.round(F.col('merchant_fraud_probability_%'), 4))

merch_fp_clean = df

######################################## EXTERNAL ########################################

# 2021 census data CSVs

df = sa2_census

df.write.mode('overwrite').parquet("data/nulls&missing_analysis/sa2/sa2_census.parquet")

# 1. Remove Null Values
df = df.na.drop()

# 2. Remove Duplicates
df = df.dropDuplicates()

# 3. Validate sa2_code
df = df.filter(F.col('sa2_code') > 0)

# 4. Validate median_age
df = df.filter(F.col('sa2_median_age') >= 0)

# 5. Validate median mortgage repay monthly
df = df.filter(F.col('sa2_median_mortgage_repay_monthly') >= 0)

# 6. Validate median total personal weekly income
df = df.filter(F.col('sa2_median_tot_prsnl_inc_weekly') >= 0)

# 7. Validate median weekly rent
df = df.filter(F.col('sa2_median_rent_weekly') >= 0)

# 8. Validate median total family weekly income
df = df.filter(F.col('sa2_median_tot_fam_inc_weekly') >= 0)

# 9. Validate average number of persons per bedroom
df = df.filter(F.col('sa2_average_num_psns_per_bedroom') >= 0)

# 10. Validate median total household weekly income
df = df.filter(F.col('sa2_median_tot_hhd_inc_weekly') >= 0)

# 11. Validate average household size
df = df.filter(F.col('sa2_average_household_size') >= 0)

sa2_census_clean = df


#========================================================================================#

# SA2 population data

df = sa2_pops

df.write.mode('overwrite').parquet("data/nulls&missing_analysis/sa2/sa2_pops.parquet")

# 1. Remove Null Values
df = df.na.drop()

# 2. Remove Duplicates
df = df.dropDuplicates()

# 3. Validate population
df = df.filter(F.col('population_2021') >= 0)

sa2_pops_clean = df




#========================================================================================#

# Locality to SA2 coding index

df = sa2_to_postcode

df.write.mode('overwrite').parquet("data/nulls&missing_analysis/sa2/sa2_to_postcode.parquet")

# 1. Remove Null Values
df = df.na.drop()

# 2. Remove Duplicates
df = df.dropDuplicates()

# No need to validate postcode as inner join will be performed with validated postcodes

sa2_to_postcode_clean = df


######################################## AGGREGATION ########################################

print("Aggregating data...")

# Join the original datasets
orig_combined = cust_tbl_clean.join(cust_user_det_clean, on='consumer_id', how='inner') \
.join(transactions_all_clean, on='user_id', how ='inner') \
.join(cust_fp_clean, on=['user_id', 'order_datetime'], how='left').na.fill(0) \
.join(merch_tbl_clean, on='merchant_abn', how='inner') \
.join(merch_fp_clean, on=['merchant_abn', 'order_datetime'], how='left').na.fill(0)



# Join the SA2 data
sa2_combined = sa2_to_postcode_clean.join(sa2_pops_clean, on='sa2_code', how='inner').withColumnRenamed('population_2021','sa2_population').join(sa2_census_clean, on='sa2_code', how='inner')

# Group SA2 statistics by postcodes by taking averages and medians

postcode_combined = sa2_combined.groupBy("postcode") \
    .agg(F.avg("sa2_population").alias("consumer_postcode_estimated_population"), \
        F.median("sa2_median_age").alias("consumer_postcode_median_age"), \
        F.median("sa2_median_mortgage_repay_monthly").alias("consumer_postcode_median_mortgage_repay_monthly"), \
        F.median("sa2_median_tot_prsnl_inc_weekly").alias("consumer_postcode_median_totl_prsnal_inc_weekly"), \
        F.median("sa2_median_rent_weekly").alias("consumer_postcode_median_rent_weekly"), \
        F.median("sa2_median_tot_fam_inc_weekly").alias("consumer_postcode_median_tot_fam_inc_weekly"), \
        F.avg("sa2_average_num_psns_per_bedroom").alias("consumer_postcode_avg_num_psns_per_bedroom"), \
        F.median("sa2_median_tot_hhd_inc_weekly").alias("consumer_postcode_median_tot_hhd_inc_weekly"), \
        F.avg("sa2_average_household_size").alias("consumer_postcode_avg_household_size")
    ).withColumnRenamed('postcode', 'consumer_postcode')


postcode_combined = postcode_combined.select(*[F.round(c, 2).alias(c) for c in postcode_combined.columns])

postcode_combined = postcode_combined.withColumn("consumer_postcode_estimated_population", F.round(F.col("consumer_postcode_estimated_population")))

# Combine everything together

all_combined = orig_combined.join(postcode_combined.withColumnRenamed('postcode','consumer_postcode'), on='consumer_postcode', how='inner')

# Last adjustments to column names

all_combined = all_combined.withColumnRenamed(
    'words',
    'merchant_description'
).withColumnRenamed(
    'revenue_level',
    'merchant_revenue_level'
).withColumnRenamed(
    'take_rate_%',
    'merchant_take_rate_%'
).withColumnRenamed(
    'dollar_value',
    'transaction_dollar_value_$AUD'
)

print("Saving to data/curated/...")

all_combined.write.mode('overwrite').parquet("data/curated/all_data_combined.parquet")

#endregion

spark.stop()

print("ETL Script Complete.")