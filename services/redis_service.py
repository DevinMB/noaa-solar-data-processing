import redis
import os
from dotenv import load_dotenv

load_dotenv()


REDIS_HOST = os.getenv('REDIS_HOST')  
REDIS_PORT = os.getenv('REDIS_PORT')  

def write_to_redis_hashset(username, password, key, mapping):
    redis_client = redis.StrictRedis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        username=username,
        password=password,
        decode_responses=True
    )
    try:
        redis_client.hmset(key, mapping)
        print(f"Hashset mapping written to Redis successfully: {key} : {mapping}")
    except Exception as e:
        print(f"Failed to write to Redis: {e}")


