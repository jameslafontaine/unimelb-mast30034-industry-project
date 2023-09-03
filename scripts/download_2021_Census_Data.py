import os
from urllib.request import urlretrieve
import zipfile

output_dir = 'data'

if not os.path.exists(output_dir):
    print("Could not find directory")
else:
    os.makedirs(output_dir + "/externaldataset", exist_ok=True)
    print("Download Start")
    output_file = output_dir + "/externaldataset/2021_GCP_SA2_for_AUS_short-header.zip"
    url = "https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_SA2_for_AUS_short-header.zip"
    urlretrieve(url, output_file)
    print("Download Complete")

files_to_extract = ["2021Census_G02_AUST_SA2.csv", "2021Census_G33_AUST_SA2.csv"]

zip_file_path = "data/externaldataset/2021_GCP_SA2_for_AUS_short-header.zip"

destination_folder = "data/externaldataset"

with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    for file_name in files_to_extract:
        try:
            zip_ref.extract(f"2021 Census GCP Statistical Area 2 for AUS/{file_name}", destination_folder)
            print(f"Extracted: {file_name}")
        except KeyError:
            print(f"File not found in the zip archive: {file_name}")

print("Extraction completed.")
