import functions_framework
from google.cloud import dataproc_v1
import time
from datetime import datetime
from zoneinfo import ZoneInfo 

# Define constants
PROJECT_ID = "sylvan-mode-413619"
REGION = "europe-west3"
ZONE = "europe-west3-c"
CLUSTER_NAME = "spotify-dataproc-cluster"
PYSPARK_JOB = "gs://spotify_pyspark_job/pyspark_job.py"
STARTUP_SCRIPT = "gs://dataproc_initialization_script_bucket/connectors.sh"

# Main function triggered from a HTTP request from Cloud Scheduler
@functions_framework.http
def run_dataproc_job(request):
    try:
        # Define the time zone
        cet_timezone = ZoneInfo('Europe/Copenhagen')
        # Get the current date and time
        current_datetime = datetime.now(cet_timezone)
        current_date = current_datetime.strftime('%Y-%m-%d')
        current_time = current_datetime.strftime('%H:%M')
        # Start timer
        start_time = time.time()
        
        print(f" -----------------------------------------------------\n Script execution started on {current_date} at {current_time} \n -----------------------------------------------------")
        
        # Initialize clients
        cluster_client = dataproc_v1.ClusterControllerClient(client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"})
        job_client = dataproc_v1.JobControllerClient(client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"})
        
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

        # Create the cluster
        operation = cluster_client.create_cluster(
            request={"project_id": PROJECT_ID, "region": REGION, "cluster": cluster_config}
        )
        response = operation.result()
        print(f"Cluster {CLUSTER_NAME} created.")

        # Wait for cluster to be ready
        time.sleep(60)

        # Submit the PySpark job
        job = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": PYSPARK_JOB},
        }
        operation = job_client.submit_job_as_operation(request={"project_id": PROJECT_ID, "region": REGION, "job": job})
        response = operation.result()
        print(f"Job finished with status: {response.status.state}")

        # Delete the cluster
        delete_operation = cluster_client.delete_cluster(project_id=PROJECT_ID, region=REGION, cluster_name=CLUSTER_NAME)
        delete_operation.result()
        print(f"Cluster {CLUSTER_NAME} deleted.")
        
        # End timer
        end_time = time.time()
        # Calculate execution time
        total_time = end_time - start_time
        
        print(f"-----------------------------------------------------\n Total execution time: {total_time/60} minutes \n-----------------------------------------------------")
        
        # Return success response
        return f"Job completed successfully in {total_time / 60:.2f} minutes.", 200

    except Exception as e:
        # Log and return error response
        print(f"Error occurred: {e}")
        return f"Error occurred: {e}", 500
