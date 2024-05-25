import zipfile
import os


zip_file = "data/SA2_2021_AUST_SHP_GDA2020.zip"
output_dir = "data"

# Unzip the file
with zipfile.ZipFile(zip_file, 'r') as zip_ref:
    zip_ref.extractall(output_dir)

