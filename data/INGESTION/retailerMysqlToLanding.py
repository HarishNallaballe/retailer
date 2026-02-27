from pyspark.sql import SparkSession
from google.cloud import storage,bigquery
from datetime import datetime 
import json
import pandas as pd

bq_client = bigquery.Client()
storage_client = storage.Client()

spark = SparkSession.builder.appName("RetailerToLanding").getOrCreate()

GCS_BUCKET = "retailer-datalake-project-24022026"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/retailer-db/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/retailer-db/archive/"

CONFIG_PATH = f"gs://{GCS_BUCKET}/configs/retailer_config.csv"


#### jdbc config
database = "retailerDB"
jdbc_url = f"jdbc:mysql://136.112.246.242:3306/{database}" 
user = "harish"
password = "Harish@123"
driver = "com.mysql.cj.jdbc.Driver"

# MySQL Configuration
MYSQL_CONFIG = {
    "url": "jdbc:mysql://136.112.246.242:3306/retailerDB?useSSL=true&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "harish",
    "password": "Harish@123"
}


##### bigquery config
BQ_PROJECT_ID = "project-b4eb31f9-d0fb-422f-8d7"
BQ_AUDIT_TABLE = f"{BQ_PROJECT_ID}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT_ID}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

log_entries = []

def log_event(event_type,message,table=None):
    log_entry = {
        "timestamp" : datetime.now().isoformat(),
        "event_type" : event_type,
        "message" : message,
        "table" : table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def save_log_to_gcs():
    log_filename = f"pipeline_log_{datetime.now().strftime('%y%m%d%M%H')}.json"
    log_filepath = f"temp/pipeline_log/{log_filename}"
    json_data = json.dumps(log_filename,indent=4)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json_data,content_type="application/json")
    print(f"✅ Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")

def save_log_to_bigquery():
    if log_entries:
        df = spark.createDataFrame(log_entries)
        df.write.format("bigquery").mode("append").option("table",BQ_LOG_TABLE).option("temporaryGcsBucket",BQ_TEMP_PATH).save()
        print("✅ Logs stored in BigQuery for future analysis")


def move_existing_files_to_archive(table):
    blobs = (storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/retailer-db/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return
    
    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)
        # date_part = file.split('-')[-1].split('.')[0]

        date_part = file.split("_")[-1].split(".")[0]
        year = date_part[-4:]
        month = date_part[2:4]
        day = date_part[:2]

        archive_path = f"landing/retailer-db/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        #destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob,storage_client.bucket(GCS_BUCKET),archive_path)
        source_blob.delete()
        
        log_event("INFO", f"✅ Moved {file} to {archive_path}", table=table)    


df_config = spark.read.format("csv").option("header","true").load(CONFIG_PATH)

def get_latest_watermark(tablename:str):
    query = f"""
        select max(load_timestamp) AS latest_timestamp
        from `{BQ_AUDIT_TABLE}` 
        where tablename = '{tablename}'
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"

def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        # Get Latest Watermark
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)
        
        # Generate SQL Query
        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full load" else \
                f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"
        
        # Read Data from MySQL
        df = (spark.read
                .format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query)
                .load())
        log_event("SUCCESS", f"✅ Successfully extracted data from {table}", table=table)
        
        # Convert Spark DataFrame to JSON
        pandas_df = df.toPandas()
        json_data = pandas_df.to_json(orient="records", lines=True)
        
        # Generate File Path in GCS
        today = datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/retailer-db/{table}/{table}_{today}.json"
        
        # Upload JSON to GCS
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(json_data, content_type="application/json")

        log_event("SUCCESS", f"✅ JSON file successfully written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)
        
        # Insert Audit Entry
        audit_df = spark.createDataFrame([
            (table, load_type, df.count(), datetime.now(), "SUCCESS")], ["tablename", "load_type", "record_count", "load_timestamp", "status"])

        (audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .mode("append")
            .save())

        log_event("SUCCESS", f"✅ Audit log updated for {table}", table=table)
    
    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)
        raise

cols = ["database", "tablename", "loadtype", "watermark", "is_active"]

for row in df_config.collect():
    if row["is_active"] == '1':
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_log_to_gcs()
save_log_to_bigquery()
print("✅ Pipeline completed successfully!")