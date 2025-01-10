import base64
import requests
import time
from datetime import datetime
from zoneinfo import ZoneInfo 
import json
from google.cloud import secretmanager
from google.cloud import storage
import functions_framework

############################# AUTHENTICATION ##################################

# Function for fetching secrets from GCP Secret Manager
def get_secret(secret_id):
    try:
        # Create the Secret Manager client
        sm_client = secretmanager.SecretManagerServiceClient()
        # Build the secret name/path
        secret_name = sm_client.secret_path('sylvan-mode-413619', secret_id) + '/versions/1'
        # Access the secrets value
        secret_string = sm_client.access_secret_version(name=secret_name).payload.data.decode('utf-8')

        try:
            # Try to load the secret as JSON
            return json.loads(secret_string)
        except json.JSONDecodeError:
            # If it fails, return the secret string as is
            return secret_string
    
    except Exception as e:
        print(f"Error occurred while fetching secret from Secret Manager: {e}")
        return None
    
    
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

    
############################## DATA FETCHING ##################################

# Save JSON files to Storage Bucket
def save_to_gcs(storage_client, data, filename, bucket_name = 'spotify_json_files_bucket'):
    # Get Storage Bucket refference
    bucket = storage_client.bucket(bucket_name)
    # Create a blob in the Bucket with the assigned filename
    blob = bucket.blob(filename)
    # Upload JSON file to the blob
    blob.upload_from_string(json.dumps(data, indent=4), content_type="application/json", timeout=120)
    print(f"Saved {filename} to Storage Bucket.")

# Fetch tracks for all albums
def fetch_album_tracks(album_ids, headers):
    tracks_data = {} # Dict to store all tracks for each album
    # Loop through all album ids
    for album_id in album_ids:
        # Paste album id into base URL for album tracks and execute request
        album_tracks_url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
        response = requests.get(album_tracks_url, headers=headers)
        
        if response.status_code == 200:
            # Save track data for album
            tracks_data[album_id] = response.json().get("items", [])
        elif response.status_code == 429:  # Rate limit hit
            # Retrieve the retry time from the response header
            retry_after = int(response.headers.get("Retry-After", 1))
            print(f"Rate limit hit. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            # Retry after retry-time is over
            response = requests.get(album_tracks_url, headers=headers)  # Retry
            if response.status_code == 200:
                # Save track data for album after retry
                tracks_data[album_id] = response.json().get("items", [])
            else:
                print(f"Failed to fetch tracks for album {album_id} after retry.")
        else:
            print(f"Error fetching tracks for album {album_id}: {response.status_code}")
    return tracks_data

# Process album ids in smaller batches to avoid hitting rate limits
def batch_process_albums(album_ids, batch_size, headers):
    
    batched_tracks_data = {} # Dict to store all batch results
    
    for i in range(0, len(album_ids), batch_size):
        # Create a batch of album ids
        batch = album_ids[i:i + batch_size]
        
        # Define current batch number and total number of batches
        current_batch = i // batch_size + 1
        # Calculate total batches
        total_batches = (len(album_ids) + batch_size - 1) // batch_size  
        print(f"Processing batch {current_batch} of {total_batches}...")
        
        # Fetch track data for the current batch
        batch_tracks = fetch_album_tracks(batch, headers)
        # Merge batch data into the overall results
        batched_tracks_data.update(batch_tracks)
        # Pause to avoid hitting rate limits
        time.sleep(2) 
    return batched_tracks_data

# Main function triggered from a HTTP request from Cloud Scheduler
@functions_framework.http
def fetch_new_releases(request):
    try:
        # Define the time zone
        cet_timezone = ZoneInfo('Europe/Copenhagen')
        # Get the current date and time
        current_datetime = datetime.now(cet_timezone)
        current_date = current_datetime.strftime('%Y-%m-%d')
        current_time = current_datetime.strftime('%H:%M')

        print(f" -----------------------------------------------------\n Script execution started on {current_date} at {current_time} \n -----------------------------------------------------")

        # Start timer
        start_time = time.time()

        # Fetch the Spotify API access token and create request header
        access_token = get_spotify_token()
        headers = {"Authorization": f"Bearer {access_token}"}

        # Initialize Storage client
        storage_client = storage.Client()
        # Define base URL for fetching new album releases
        new_releases_url = "https://api.spotify.com/v1/browse/new-releases"

        # Define European markets
        markets = ["AD", "AL", "AM", "AT", "AZ", "BA", "BE", "BG", "BY", "CH", "CY",
                   "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GB", "GE", "GR", "HR",
                   "HU", "IE", "IS", "IT", "LI", "LT", "LU", "LV", "MC", "MD", "ME",
                   "MK", "MT", "NL", "NO", "PL", "PT", "RO", "RS", "SE", "SI", "SK",
                   "SM", "TR", "UA", "XK"]

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

        # End timer
        end_time = time.time()
        # Calculate execution time
        total_time = end_time - start_time

        print(f"-----------------------------------------------------\n Total execution time: {total_time / 60:.2f} minutes \n-----------------------------------------------------")

        # Return success response
        return f"Fetch new releases completed successfully in {total_time / 60:.2f} minutes.", 200

    except Exception as e:
        # Log and return error response
        print(f"Error occurred: {e}")
        return f"Error occurred: {e}", 500
