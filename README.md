# Spotify New Releases - Spark ETL Pipeline
Try the dashboard out live at: https://lookerstudio.google.com/u/0/reporting/f258ee4e-d0b1-4760-bfad-c27af9185053/page/aUmaE  

![image](https://github.com/user-attachments/assets/4695d872-c856-4db3-8a08-77c0d7de117a)

## Introduction
This project automates the extraction, transformation, and loading of Spotify's "New Releases" data for European markets into a Looker dashboard. The pipeline is designed to run weekly, fetching the latest tracks without any manual intervention. Using Google Cloud services like Cloud Run, Cloud Storage, and BigQuery, combined with PySpark on Dataproc, the pipeline efficiently processes raw API data into a format suitable for visualization.

## Table of Contents
- [Introduction](#introduction)
- [Project Diagram](#project-diagram)
- [Raw Data Ingestion](#raw-data-ingestion)
  - [Authentication and API Token Management](#authentication-and-api-token-management)
  - [Data Fetching](#data-fetching)
  - [JSON Files Storage](#json-files-storage)
- [Data Transformation](#data-transformation)
  - [Dataproc Spark Job Automation](#dataproc-spark-job-automation)
    - [Cluster Creation](#cluster-creation)
    - [Startup Script](#startup-script)
    - [Job Submitting](#job-submitting)
    - [Cluster Deletion](#cluster-deletion)
  - [PySpark Job](#pyspark-job)
    - [JSON Files Import](#json-files-import)
    - [PySpark Data Transformation](#pyspark-data-transformation)
    - [Writing Data to BigQuery](#writing-data-to-bigquery)
- [Technologies](#technologies)

## Project Diagram
![image](https://github.com/user-attachments/assets/bb90591f-fefe-4f85-a89c-cb5508099c91)

## Raw Data Ingestion
The ingestion of the raw data from the Spotify API happens in the 'ingestion_function.py' which is deployed as a Cloud Run Function. This Function is invoked every monday at 12 AM by a seperate Cloud Scheduler.  
The data ingestion concists of three overall parts - authentication, fetching, and storage.  

### Authentication and API Token Management
Before we can do anything with the API, we need to authenticate and obtain an access token first. And for that we need a client ID and client secret.
To store these securely I used the GCP Secret Manager. That way they could be accessed by the script, using the 'get_secret' function. 
The access token for the Spotify API would then be created with the following function:  
```python
def get_spotify_token():
    # Load Spotify client id and secret
    client_id = get_secret('SPOTIFY_CLIENT_ID')
    client_secret = get_secret('SPOTIFY_CLIENT_SECRET')
    # Concatenate client id and secret to a single string
    client_creds = f"{client_id}:{client_secret}"
    # Encode the string to base64
    client_creds_b64 = base64.b64encode(client_creds.encode()).decode()

    # Define token url and headers for the request
    token_url = 'https://accounts.spotify.com/api/token'
    headers = {'Authorization': f'Basic {client_creds_b64}',
               'Content-Type': 'application/x-www-form-urlencoded'}
    # Define request body parameters
    data = {'grant_type': 'client_credentials'}
    
    # Send request
    response = requests.post(token_url, headers=headers, data=data)
    
    # Parse response and get access token
    if response.status_code == 200:
        token_info = response.json()
        return token_info['access_token']
    else:
        raise Exception(f"Failed to get Spotify token: {response.status_code} - {response.text}")
```  
### Data Fetching
Data is fetched for each European market, with a limit of 50 albums per market. The albums are then processed in batches to fetch all tracks within the albums. Batch processing is used to prevent hitting the APIs (rather strict) rate limit.
The functions that are responsible for the data fetching is 'fetch_album_tracks' and 'batch_process_albums'. Their utilization in the main function can be seen in the snippet below:  
```python
# Loop through all defined markets to fetch data
for market in markets:
    # Define parameters for the API request and execute
    params = {"limit": 50, "market": market}
    response = requests.get(new_releases_url, headers=headers, params=params)

    if response.status_code == 200:
        # Parse the album data from the response
        data = response.json()
        albums = data.get("albums", {}).get("items", [])
        # Get album id for each album
        album_ids = [album["id"] for album in albums]

        print(f"Fetching tracks for {len(album_ids)} albums in market {market}...")
        # Process albums in batches of 10
        batch_size = 10
        batched_tracks = batch_process_albums(album_ids, batch_size, headers)

        # Enrich album data with tracks
        enriched_albums = []
        for album in albums:
            album_id = album["id"]
            album["tracks"] = batched_tracks.get(album_id, [])
            enriched_albums.append(album)

        # Save to Storage Bucket
        filename = f"spotify_new_releases_{market}.json"
        # Append a dict containing both data + market
        final_market_file = {
            "data": enriched_albums,
            "market": market
        }
        save_to_gcs(storage_client, final_market_file, filename)
        print(f"Fetched and saved data for market: {market}")
    else:
        print(f"Error fetching data for market {market}: {response.status_code} - {response.text}")

    # Short pause between markets to avoid hitting rate limit
    time.sleep(5)  
```  

### JSON Files Storage
The final part of the raw data ingestion is to store the data from each market into a Cloud Storage Bucket. The final data for each market is essentially a Python dictationary with a key for the market name, and a key for the list containing the fetched data. This is then uploaded as a JSON file. The function that uploads the data can be seen below. The utilization of this function can be seen at the end of the snippet above, as it's part of the same for loop.  
```python
# Save JSON files to Storage Bucket
def save_to_gcs(storage_client, data, filename, bucket_name = 'spotify_json_files_bucket'):
    # Get Storage Bucket refference
    bucket = storage_client.bucket(bucket_name)
    # Create a blob in the Bucket with the assigned filename
    blob = bucket.blob(filename)
    # Upload JSON file to the blob
    blob.upload_from_string(json.dumps(data, indent=4), content_type="application/json", timeout=120)
    print(f"Saved {filename} to Storage Bucket.")
```  

## Data Transformation
Once the data is stored in Cloud Storage as raw JSON files, it needs to be processed and transformed. That will need some computational power and some efficient data wrangling. For that I used Dataproc and PySpark.

### Dataproc Spark Job Automation
To gain the computational power needed to run my PySpark script, I decided to use Dataproc clusters. However, since the script was only supposed to run once a week in a short period of time, I decided to automate the cluster creation, Spark job submitting, and cluster deletion. This was done using a Cloud Run Function. That way Dataproc wouldn't incur any unnecessary costs, while no clusters were being used. The full code that was deployed, can be seen in 'dataproc_function.py'.

#### Cluster Creation
Before a Dataproc cluster is created, it needs to be configured correctly and for the specific use case. In this case I configured the cluster like so:  
```python
# Cluster configuration with startup script
cluster_config = {
    "project_id": PROJECT_ID,
    "cluster_name": CLUSTER_NAME,
    "config": {
        "gce_cluster_config": {
            "zone_uri": f"projects/{PROJECT_ID}/zones/{ZONE}",
            "metadata": {
                "startup-script-url": STARTUP_SCRIPT  # Include the startup script
            },
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "e2-standard-2",
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "e2-standard-2",
        },
    },
}
```
I chose the e2-standard-2 machine type for the master and worker nodes due to its small size, low cost, and good performance, making it well-suited for the task.

##### Startup Script
The cluster configuration also points to a startup script URL, which essentially is a Cloud Storage Bucket that contains a .sh file, which is the startup script. It is responsible for installing and configuring the BigQuery connectors for Hadoop, Spark, and Hive on a Dataproc cluster. This makes it possible for the PySpark script to write data to BigQuery, later in the process. The connector for Cloud Storage comes pre-installed with Dataproc clusters, which makes it possible for the PySpark script to read the JSON files from the bucket without additional setup.

#### Job Submitting
The PySpark job is submitted like so:
```python
# Submit the PySpark job
job = {
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_JOB},
}
operation = job_client.submit_job_as_operation(request={"project_id": PROJECT_ID, "region": REGION, "job": job})
```
The job points to the actual Python file stored in a Cloud Storage bucket (named 'pyspark_job.py' in this repository).

#### Cluster Deletion
Once the PySpark job is finished, the cluster is deleted to avoid any unnecessary costs.
```python
# Delete the cluster
delete_operation = cluster_client.delete_cluster(project_id=PROJECT_ID, region=REGION, cluster_name=CLUSTER_NAME)
```

### PySpark Job
The 'pyspark_job.py' file is the one that's responsible for the data transformation with Spark. This is where the raw JSON files gets transformed to a PySpark DataFrame and exported to BigQuery.

#### JSON Files Import
We need to read the JSON files from the Cloud Storage bucket they are stored in. To do that, I used the 'spark.read' function.
```python
# Read JSON files from GCS with multiline option
logger.info(f"Reading JSON files from GCS bucket: {BUCKET_NAME}")
raw_df = spark.read \
    .option("multiline", "true") \
    .schema(schema) \
    .json(f"gs://{BUCKET_NAME}/*.json")
logger.info(f"Successfully read JSON files. Row count: {raw_df.count()}")
```
The '("multiline", "true")' option was used to ensure that each file and its lines were read as a single JSON object. Otherwise, each line in each file would have been read as a complete, separate JSON object.

The read function above also utilizes a schema that defines the structure of the data in the JSON files. It defines the fields, data types and nesting levels.
```python
# Define the schema for the JSON data
schema = StructType([
    StructField("market", StringType(), True),
    StructField("data", ArrayType(StructType([
        StructField("release_date", StringType(), True),
        StructField("tracks", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("duration_ms", IntegerType(), True),
            StructField("artists", ArrayType(StructType([
                StructField("name", StringType(), True)
            ])), True)
        ])), True)
    ])), True)
])
```
Providing a schema upfront, as I have done here, skips the need for Spark to infer the schema dynamically, which improves computational performance. Furthermore, it also facilitates easier querying and transformations by enabling the use of Spark SQL functions like explode, select, and withColumn.

#### PySpark Data Transformation
Once all the JSON files have been fetched into a PySpark DataFrame, it needs to be flattened properly, so that the end result is a tabular DataFrame.  

I started by exploding the 'data' array so that each row represents a single album and its market.
```python
 albums_df = raw_df.select(
    col("market"),
    explode(col("data")).alias("album_data")
)
```
Then I exploded the 'tracks' array, which creates a row for each track and its release date.
```python
tracks_df = albums_df.select(
    col("market"),
    col("album_data.release_date").alias("release_date"),
    explode(col("album_data.tracks")).alias("track_data")
)
```
All relevant fields are extracted. The last line extracts the names of all artists in the 'artists' array for each track and stores them as a new array in a column named 'artist_names'.
```python
final_tracks_df = tracks_df.select(
    col("market"),
    col("release_date"),
    col("track_data.name").alias("track_name"),
    col("track_data.duration_ms").alias("duration_ms"),
    expr("transform(track_data.artists, x -> x.name)").alias("artist_names")
)
```
All the artist names for each track are combined into a single string and inserted into the newly created 'artists' column. The previous 'artist_names' column is dropped.
```python
final_tracks_df = final_tracks_df.withColumn(
    "artists",
    concat_ws(", ", col("artist_names"))
).drop("artist_names")
```
Finally, a 'load_date' column is created, using the 'current_date()' function.
```python
final_tracks_df = final_tracks_df.withColumn("load_date", current_date())
```

#### Writing Data to BigQuery
In the last step, the final PySpark DataFrame is written to BigQuery.
```python
# Write the processed data to BigQuery
logger.info(f"Writing data to BigQuery table: {BIGQUERY_TABLE}")
final_tracks_df.write \
    .format("bigquery") \
    .option("writeMethod", "direct") \
    .mode("append") \
    .save(BIGQUERY_TABLE)
logger.info("Data successfully written to BigQuery.")
```
I used the 'append' mode because I wanted to add data to the existing table, instead of overwriting it. 

## Technologies
This project is built using:
- Python: Main language for building all scripts.
- Spark/PySpark: Data transformation.
- Spotify API: Data source for Spotify's "New Releases".
- Dataproc: Spark clusters for computational power.
- Google Cloud Storage: Storing JSON files, Spark startup script, and PySpark job script.
- Google Cloud Run Functions: Deployment and automation of Python scripts.
- Google Cloud Scheduler: Scheduling and activation of Cloud Run Functions.
- Google Cloud Secret Manager: Secure storage and management of API keys.
- BigQuery: Data storage.
- Looker Studio: Data visualization.
