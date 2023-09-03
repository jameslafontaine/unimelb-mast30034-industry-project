import os
from urllib.request import urlretrieve
import zipfile

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
