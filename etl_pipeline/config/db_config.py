from pymongo import MongoClient
import psycopg2

# MongoDB Configuration
MONGO_URI = "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017"
mongo_client = MongoClient(MONGO_URI)
mongo_db_kg = mongo_client["knowledge_graph"]  # MongoDB for wallets
mongo_db_transactions = mongo_client["ethereum_blockchain_etl"]  # MongoDB for transactions

# PostgreSQL Configuration
pg_conn = psycopg2.connect(
    "postgresql://target_user:target_pass@localhost:5432/target_db"
)
pg_cursor = pg_conn.cursor()

def close_connections():
    pg_cursor.close()
    pg_conn.close()
    mongo_client.close()
