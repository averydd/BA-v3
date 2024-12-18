from pymongo import MongoClient
import pandas as pd

# MongoDB Connection String
mongo_uri = "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017"

# Connect to MongoDB
client = MongoClient(mongo_uri)

# Select the database and collections
db = client["ethereum_blockchain_etl"]
db_kg = client["knowledge_graph"] # Database name

# 1. Transactions Collection - Fetch Key Fields
transactions_collection = db["transactions"]

transactions_query = {}
transactions_projection = {
    "_id": 1,
    "hash": 1,
    "from_address": 1,
    "to_address": 1,
    "value": 1,
    "block_timestamp": 1,
    "block_number": 1,
    "receipt_status": 1
}

transactions = transactions_collection.find(transactions_query, transactions_projection).limit(10)
transactions_df = pd.DataFrame(list(transactions))
print(transactions_df)

# 2. Events Collection - Fetch Key Fields
from pymongo import MongoClient
import pandas as pd

# MongoDB connection
mongo_uri = "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017"
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client["ethereum_blockchain_etl"]
events_collection = mongo_db["events"]

# Query for all SWAP, MINT, and BURN events
events_query = {"event_type": {"$in": ["SWAP", "MINT", "BURN"]}}

# Fetch all records with no field projection
events = events_collection.find(events_query).limit(1000)
events_df = pd.DataFrame(list(events))

# Print first 5 rows to verify
print("First 5 rows of the DataFrame:")
print(events_df.head())

# Check the total columns and unique field names
print("\nAll columns in the events DataFrame:")
print(events_df.columns)

# Separate counts of fields for SWAP, MINT, and BURN
def column_presence_by_event_type(df, event_type):
    """ Count how many records contain each column for a specific event type. """
    subset = df[df['event_type'] == event_type]
    field_counts = {col: subset[col].notnull().sum() for col in df.columns}
    field_counts_df = pd.DataFrame(field_counts.items(), columns=["Field", "Count"])
    field_counts_df = field_counts_df.sort_values(by="Count", ascending=False)
    return field_counts_df

# Get field counts for each event type
print("\nField presence for SWAP events:")
swap_fields = column_presence_by_event_type(events_df, "SWAP")
print(swap_fields)

print("\nField presence for MINT events:")
mint_fields = column_presence_by_event_type(events_df, "MINT")
print(mint_fields)

print("\nField presence for BURN events:")
burn_fields = column_presence_by_event_type(events_df, "BURN")
print(burn_fields)


wallets_collection = db_kg["wallets"]
wallets_query = {"balanceInUSD": {"$gte": 1000000}}  # Wallets with >= $1M
wallets_projection = {
    "_id": 1,
    "address": 1,
    "balanceInUSD": 1,
    "tokens": 1,
    "dailyAllTransactions": 1,
    "dailyTransactionAmounts": 1,
    "numberOfLiquidation": 1
}

wallets = wallets_collection.find(wallets_query, wallets_projection).limit(10)
wallets_df = pd.DataFrame(list(wallets))
print(wallets_df)
