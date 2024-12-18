from config.db_config import mongo_client, pg_conn, pg_cursor
from utils.logger import log_info
from models.transactions import transactions_coll

# MongoDB and PostgreSQL setup
db = mongo_client["ethereum_blockchain_etl"]
dex_events_coll = db["dex_events"]

def create_dex_events_table():
    create_table_query = """
        CREATE TABLE IF NOT EXISTS dex_events (
            _id VARCHAR PRIMARY KEY,
            transaction_hash VARCHAR NOT NULL REFERENCES transactions(hash),
            block_number INT,
            block_timestamp TIMESTAMP,
            contract_address VARCHAR,
            event_type VARCHAR,
            log_index INT,
            wallet VARCHAR REFERENCES wallets(address),
            sender VARCHAR REFERENCES wallets(address),
            to_address VARCHAR REFERENCES wallets(address),
            amount0In NUMERIC,
            amount0Out NUMERIC,
            amount1In NUMERIC,
            amount1Out NUMERIC
        );
    """
    pg_cursor.execute(create_table_query)
    pg_conn.commit()
    log_info("Table 'dex_events' created successfully.")

def validate_transactions_for_dex_events(dex_events_batch):
    """
    Validates that 'transaction_hash' exists in MongoDB transactions.
    """
    # Extract unique transaction hashes from the batch
    transaction_hashes = {event["transaction_hash"] for event in dex_events_batch}

    # Query MongoDB transactions for these hashes
    existing_transactions = transactions_coll.find(
        {"hash": {"$in": list(transaction_hashes)}}, {"hash": 1}
    )
    existing_transaction_set = {txn["hash"] for txn in existing_transactions}

    # Filter dex events where the transaction exists
    valid_dex_events = [
        event for event in dex_events_batch
        if event["transaction_hash"] in existing_transaction_set
    ]

    return valid_dex_events


def run_dex_events_etl(batch_size=500):
    """
    Process one batch of dex events and return True if data was processed, False otherwise.
    """
    create_dex_events_table()
    dex_events_processed = etl_dex_events_in_batches(batch_size=batch_size)
    return dex_events_processed

def etl_dex_events_in_batches(batch_size=500):
    try:
        log_info("Processing dex events...")
        insert_dex_event_query = """
            INSERT INTO dex_events (_id, transaction_hash, block_number, block_timestamp, contract_address, event_type, log_index, wallet, sender, to_address, amount0In, amount0Out, amount1In, amount1Out)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (_id) DO NOTHING;
        """

        skip = 0
        total_inserted = 0

        while True:
            # Fetch batch of dex events from MongoDB
            dex_events_cursor = dex_events_coll.find({}, skip=skip, limit=batch_size)
            dex_events_batch = list(dex_events_cursor)

            if not dex_events_batch:
                break

            # Validate referenced transactions in MongoDB
            valid_dex_events = validate_transactions_for_dex_events(dex_events_batch)

            records_inserted = 0
            for event in valid_dex_events:
                try:
                    # Insert into PostgreSQL
                    pg_cursor.execute(insert_dex_event_query, (
                        event["_id"],
                        event["transaction_hash"],
                        event.get("block_number"),
                        event.get("block_timestamp"),
                        event.get("contract_address"),
                        event.get("event_type"),
                        event.get("log_index"),
                        event.get("wallet"),
                        event.get("sender"),
                        event.get("to_address"),
                        event.get("amount0In"),
                        event.get("amount0Out"),
                        event.get("amount1In"),
                        event.get("amount1Out"),
                    ))
                    records_inserted += 1

                except Exception as e:
                    log_info(f"Error inserting dex event {event['_id']}: {e}")

            pg_conn.commit()
            total_inserted += records_inserted
            log_info(f"Inserted {records_inserted} valid dex events in batch. Total so far: {total_inserted}")
            skip += batch_size

        log_info(f"Total dex events inserted: {total_inserted}")
    except Exception as e:
        log_info(f"ETL Dex Events failed: {e}")
