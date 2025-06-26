import os
import json
import re
import requests
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array_contains
#========================================================================================================================#
# Path Locations
JSON_PATH = r"C:\Users\SufianRahim\Desktop\CMS_Metastore.json"
OUTPUT_FOLDER = r"C:\Users\SufianRahim\Desktop\processed_csvs"
LAST_RUN_FILE = r"C:\Users\SufianRahim\Desktop\last_run.json"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
#========================================================================================================================#


#Checks Last Run Date

if os.path.exists(LAST_RUN_FILE):
    with open(LAST_RUN_FILE, 'r') as f:
        last_run = json.load(f).get("last_run")
else:
    last_run = "2000-01-01"  # default for first run

last_run_dt = datetime.strptime(last_run, "%Y-%m-%d")

#========================================================================================================================#

#Data Pipeline

spark = SparkSession.builder \
    .appName("CMS Hospital Dataset Processor") \
    .getOrCreate()

df = spark.read.option("multiLine", True).json(f"file:///{JSON_PATH.replace(os.sep, '/')}") #Reads into Spark in pretty printed format

# Filter for Hospital-Themed Datasets & Modified Since Last Run 
df_filtered = df.filter(
    array_contains(col("theme"), "Hospitals") &
    (col("modified") > last_run)
)

# Explode the distribution array to get individual CSV download URLs so we can process each link 
df_flat = df_filtered.withColumn("dist", explode("distribution"))

# Selected the columns we need for our cleaning and output file
df_urls = df_flat.select(
    col("title"),
    col("modified"),
    col("dist.downloadURL").alias("csv_url")
)

# Convert Spark DataFrame to Pandas so we can loop through it 
csv_entries = df_urls.toPandas().to_dict(orient="records")

# Column header cleaning 
def to_snake_case(name):
    name = name.strip()
    name = re.sub(r'[^\w\s]', '', name)     # remove special characters
    name = re.sub(r'\s+', '_', name)        # replace spaces with underscores
    return name.lower()

# since running locally on windows, we use requests here to download 
def download_and_clean_csv(entry):
    title = entry["title"]
    url = entry["csv_url"]
    safe_title = title.replace(" ", "_").replace("/", "_")
    file_path = os.path.join(OUTPUT_FOLDER, f"{safe_title}.csv")

    try:
        print(f"Downloading: {title}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        df = pd.read_csv(pd.compat.StringIO(response.text))
        df.columns = [to_snake_case(col) for col in df.columns]
        df.to_csv(file_path, index=False)
        print(f"Saved: {file_path}")
    except Exception as e:
        print(f"Failed to process {url}: {e}")

# Parallel Downloads
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(download_and_clean_csv, csv_entries)

# Saves today’s date into last_run.json so the next run skips old data.
now_str = datetime.now().strftime("%Y-%m-%d")
with open(LAST_RUN_FILE, 'w') as f:
    json.dump({"last_run": now_str}, f)

print("Job Complete") 