from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws, expr, current_date
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Define the GCS bucket and BigQuery table
BUCKET_NAME = "spotify_json_files_bucket"
BIGQUERY_TABLE = "sylvan-mode-413619.spotify_new_releases.new_releases"

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

# Initialize the Spark session
logger.info("Initializing Spark session.")
spark = SparkSession.builder \
    .appName("GCS to BigQuery Pipeline") \
    .getOrCreate()

try:
    # Read JSON files from GCS with multiline option
    logger.info(f"Reading JSON files from GCS bucket: {BUCKET_NAME}")
    raw_df = spark.read \
        .option("multiline", "true") \
        .schema(schema) \
        .json(f"gs://{BUCKET_NAME}/*.json")
    logger.info(f"Successfully read JSON files. Row count: {raw_df.count()}")


    # Explode the data array to process each album
    logger.info("Exploding album data.")
    albums_df = raw_df.select(
        col("market"),
        explode(col("data")).alias("album_data")
    )
    logger.info(f"Processed albums. Row count: {albums_df.count()}")

    # Explode the tracks array to process each track
    logger.info("Exploding track data.")
    tracks_df = albums_df.select(
        col("market"),
        col("album_data.release_date").alias("release_date"),
        explode(col("album_data.tracks")).alias("track_data")
    )
    logger.info(f"Processed tracks. Row count: {tracks_df.count()}")

    # Extract the desired fields, aggregating the artists into a single list
    logger.info("Extracting and aggregating track fields.")
    final_tracks_df = tracks_df.select(
        col("market"),
        col("release_date"),
        col("track_data.name").alias("track_name"),
        col("track_data.duration_ms").alias("duration_ms"),
        expr("transform(track_data.artists, x -> x.name)").alias("artist_names")
    )

    # Combine multiple artist names into a single string per track
    final_tracks_df = final_tracks_df.withColumn(
        "artists",
        concat_ws(", ", col("artist_names"))
    ).drop("artist_names")
    
    # Add the load date column
    final_tracks_df = final_tracks_df.withColumn("load_date", current_date())
    
    logger.info(f"Final tracks processed. Row count: {final_tracks_df.count()}")

    # Write the processed data to BigQuery
    logger.info(f"Writing data to BigQuery table: {BIGQUERY_TABLE}")
    final_tracks_df.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save(BIGQUERY_TABLE)
    logger.info("Data successfully written to BigQuery.")

except Exception as e:
    logger.error(f"An error occurred: {e}", exc_info=True)
finally:
    logger.info("Stopping Spark session.")
    spark.stop()
