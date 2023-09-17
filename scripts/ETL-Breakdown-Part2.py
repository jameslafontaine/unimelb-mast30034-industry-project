###################################################
## ETL Script for BNPL Industry Project Datasets ##
##              Breakdown Part 2                 ##
##          Author: James La Fontaine            ##
##          Edited by: Dulan Wijeratne           ##
##           Last Edited: 14/09/2023             ##
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
# 'transactions.parquet' files
path = 'data/raw/tables/transactions_20210228_20210827_snapshot'
transactions_21_02_21_08 = spark.read.parquet(path)
path = 'data/raw/tables/transactions_20210828_20220227_snapshot'
transactions_21_08_22_02 = spark.read.parquet(path)
path = 'data/raw/tables/transactions_20220228_20220828_snapshot'
transactions_22_02_22_08 = spark.read.parquet(path)

######################################## CONSUMER ########################################
cust_fp = spark.read.parquet("data/checkpoint/checkpoint1/cust_fp.parquet")
cust_tbl = spark.read.parquet("data/checkpoint/checkpoint1/cust_tbl.parquet")
cust_user_det = spark.read.parquet("data/tables/consumer_user_details.parquet")

######################################## MERCHANT ########################################
merch_fp = spark.read.parquet("data/checkpoint/checkpoint1/merch_fp.parquet")
merch_tbl = spark.read.parquet("data/checkpoint/checkpoint1/merch_tbl.parquet")

######################################## EXTERNAL ########################################
sa2_census = spark.read.parquet("data/checkpoint/checkpoint1/sa2_census.parquet")
sa2_pops = spark.read.parquet("data/checkpoint/checkpoint1/sa2_pops.parquet")
sa2_to_postcode = spark.read.parquet("data/checkpoint/checkpoint1/sa2_to_postcode.parquet")

################################################
## CLEANING, FEATURE ENGINEERING, AGGREGATION ################################################
################################################

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
#df = df.filter(F.col('words') in ['NT','ACT','SA','TAS','WA','QLD','VIC','NSW'])

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
########################################################################################

#Create directory for checkpoint 2
if not os.path.exists("data/checkpoint/checkpoint2"):
    os.makedirs("data/checkpoint/checkpoint2")

print("Saving to checkpoint 2")
transactions_all_clean.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint2/transactions_all_clean.parquet")
cust_fp_clean.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint2/cust_fp_clean.parquet")
cust_tbl_clean.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint2/cust_tbl_clean.parquet")
cust_user_det_clean.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint2/cust_user_det_clean.parquet")
merch_fp_clean.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint2/merch_fp_clean.parquet")
merch_tbl_clean.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint2/merch_tbl_clean.parquet")
sa2_census_clean.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint2/sa2_census_clean.parquet")
sa2_pops_clean.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint2/sa2_pops_clean.parquet")
sa2_to_postcode_clean.write.mode("overwrite").\
    parquet("data/checkpoint/checkpoint2/sa2_tp_postcode_clean.parquet")
print("Complete save to checkpoint2")