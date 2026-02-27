from pyspark.sql import SparkSession
import requests
import json
import pandas as pd
from datetime import datetime
from google.cloud import storage


spark = SparkSession.builder.appName("CustomerReviewsAPI").getOrCreate()

API_URL = "https://699d427c83e60a406a45a4dc.mockapi.io/retailer/reviews"

response = requests.get(API_URL)

if response.status_code == 200:
    data = response.json()
    print(f"✅ Successfully fetched {len(data)} records.")
else:
    print(f"❌ Failed to fetch data. Status Code: {response.status_code}")
    exit()

df_pandas = pd.DataFrame(data)

today = datetime.today().strftime('%Y%m%d')

local_parquet_file = f"/tmp/customer_reviews_{today}.parquet"
GCS_BUCKET = "retailer-datalake-project-24022026"
GCS_PATH = f"landing/customer_reviews/customer_reviews_{today}.parquet"


# Step 5: Save Pandas DataFrame as Parquet Locally
df_pandas.to_parquet(local_parquet_file, index=False)

# Step 6: Upload Parquet File to GCS
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET)
blob = bucket.blob(GCS_PATH)
blob.upload_from_filename(local_parquet_file)

print(f"✅ Data successfully written to gs://{GCS_BUCKET}/{GCS_PATH}")
