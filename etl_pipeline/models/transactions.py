from config.db_config import mongo_client, pg_conn, pg_cursor
from utils.logger import log_info
from models.wallets import wallets_coll

# MongoDB and PostgreSQL setup
db = mongo_client["ethereum_blockchain_etl"]
transactions_coll = db["transactions"]

def create_transactions_table():
    create_table_query = """
        CREATE TABLE IF NOT EXISTS transactions (
            _id VARCHAR PRIMARY KEY,
            hash VARCHAR NOT NULL,
            from_address VARCHAR NOT NULL REFERENCES wallets(address),
            to_address VARCHAR NOT NULL REFERENCES wallets(address),
            value NUMERIC,
            block_timestamp TIMESTAMP,
            block_number INT,
            receipt_status INT
        );
    """
    pg_cursor.execute(create_table_query)
    pg_conn.commit()
    log_info("Table 'transactions' created successfully.")
    
def validate_wallets_for_transactions(transactions_batch):
    """
    Validate that `from_address` and `to_address` in transactions exist in MongoDB wallets.
    """
    try:
        # Extract unique wallet addresses from the batch
        wallet_addresses = set()
        for txn in transactions_batch:
            wallet_addresses.add(txn["from_address"])
            wallet_addresses.add(txn["to_address"])

        # Use a cursor for efficient MongoDB queries
        existing_wallets = wallets_coll.find(
            {"address": {"$in": list(wallet_addresses)}},
            {"address": 1}  # Only fetch `address` field
        )

        # Create a set of valid wallet addresses
        existing_wallets_set = {wallet["address"] for wallet in existing_wallets}

        # Filter valid transactions
        valid_transactions = [
            txn for txn in transactions_batch
            if txn["from_address"] in existing_wallets_set and txn["to_address"] in existing_wallets_set
        ]

        return valid_transactions

    except Exception as e:
        log_info(f"Validation failed: {e}")
        return []


def run_transactions_etl(batch_size=500):
    """
    Process one batch of transactions and return True if data was processed, False otherwise.
    """
    create_transactions_table()
    transactions_processed = etl_transactions_in_batches(batch_size=batch_size)
    return transactions_processed

from concurrent.futures import ThreadPoolExecutor
import pymongo

def etl_transactions_in_batches(batch_size=500, max_threads=5):
    """
    ETL pipeline for transactions with asynchronous processing.
    """
    try:
        log_info("Starting ETL for transactions...")
        insert_transaction_query = """
            INSERT INTO transactions (_id, hash, from_address, to_address, value, block_timestamp, block_number, receipt_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (_id) DO NOTHING;
        """

        skip = 0
        total_inserted = 0

        while True:
            # Fetch a batch of transactions
            transactions_cursor = transactions_coll.find({}, skip=skip, limit=batch_size)
            transactions_batch = list(transactions_cursor)

            if not transactions_batch:
                break

            # Validate wallets asynchronously
            with ThreadPoolExecutor(max_threads) as executor:
                valid_transactions = executor.submit(
                    validate_wallets_for_transactions, transactions_batch
                ).result()

            # Insert valid transactions into PostgreSQL
            records_inserted = 0
            for txn in valid_transactions:
                try:
                    pg_cursor.execute(insert_transaction_query, (
                        txn["_id"],
                        txn["hash"],
                        txn["from_address"],
                        txn["to_address"],
                        txn.get("value"),
                        txn.get("block_timestamp"),
                        txn.get("block_number"),
                        txn.get("receipt_status"),
                    ))
                    records_inserted += 1

                except Exception as e:
                    log_info(f"Error inserting transaction {txn['_id']}: {e}")

            pg_conn.commit()
            total_inserted += records_inserted
            log_info(f"Inserted {records_inserted} transactions in batch. Total so far: {total_inserted}")
            skip += batch_size

        log_info(f"Total transactions inserted: {total_inserted}")

    except Exception as e:
        log_info(f"ETL Transactions failed: {e}")
