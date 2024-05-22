import requests
import json
import time
import schedule
import sys
import os
from datetime import datetime
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.vault_service import authenticate_vault, get_redis_credentials
from services.redis_service import write_to_redis_hashset, write_to_redis_sorted_set


def fetch_data():
    url = "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json"
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad status codes
    data = response.json()
    return data

def process_and_write_data():
    try:
        # Fetch the data from the URL
        data = fetch_data()
        
        # Authenticate with Vault and get Redis credentials
        vault_client = authenticate_vault()
        role = "write"
        redis_username, redis_password = get_redis_credentials(vault_client, role)
        
        # Write each object to Redis
        for obj in data:
            time_tag = obj["time_tag"]
            # Convert time_tag to a Unix timestamp (score)
            score = int(time.mktime(time.strptime(time_tag, "%Y-%m-%dT%H:%M:%S")))
            key = f"NOAA:kp"
            value = json.dumps(obj)

            write_to_redis_sorted_set(redis_username,redis_password,key,score,value)
        
        now = datetime.now()
        formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
        print(f"{formatted_time} : Data written to Redis successfully. Will run again in 30 minutes :)")
    except Exception as e:
        print(f"An error occurred: {e}")

def job():
    process_and_write_data()

if __name__ == "__main__":
    # Schedule the job every 6 hours
    schedule.every(.5).hours.do(job)
    
    # Run the first job immediately
    job()
    
    while True:
        schedule.run_pending()
        time.sleep(1)
