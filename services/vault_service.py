import hvac
import os
from dotenv import load_dotenv

load_dotenv()


VAULT_ADDR = os.getenv('VAULT_ADDR') 
ROLE_ID = os.getenv('ROLE_ID')  
SECRET_ID = os.getenv('SECRET_ID')  
VAULT_CREDENTIALS_PATH = os.getenv('VAULT_CREDENTIALS_PATH')   

def authenticate_vault():
    client = hvac.Client(url=VAULT_ADDR, verify=False) 
    auth_response = client.auth.approle.login(
        role_id=ROLE_ID,
        secret_id=SECRET_ID
    )
    if not auth_response['auth']['client_token']:
        raise Exception("Failed to authenticate with Vault")
    client.token = auth_response['auth']['client_token']
    return client

def get_redis_credentials(client, role):
    read_response = client.read(f"{VAULT_CREDENTIALS_PATH}{role}")
    if 'data' not in read_response:
        raise Exception(f"No data found at {VAULT_CREDENTIALS_PATH}{role}")
    redis_username = read_response['data']['username']
    redis_password = read_response['data']['password']
    return redis_username, redis_password

