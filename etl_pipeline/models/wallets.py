import json  # For serializing dict fields
from config.db_config import mongo_client, pg_conn, pg_cursor
from utils.logger import log_info

# MongoDB Connection
db = mongo_client["knowledge_graph"]
wallets_coll = db["wallets"]

# Create PostgreSQL Table
def create_wallets_table():
    create_table_query = """
        CREATE TABLE IF NOT EXISTS wallets (
            _id VARCHAR PRIMARY KEY,
            address VARCHAR NOT NULL UNIQUE,
            chainId VARCHAR NOT NULL,
            balanceInUSD NUMERIC,
            tokens JSONB,
            flagged INT NOT NULL
        );
    """
    pg_cursor.execute(create_table_query)
    pg_conn.commit()
    log_info("Table 'wallets' created successfully.")

def run_wallets_etl(chain_id="0x1", batch_size=500):
    """
    Process one batch of wallets and return True if data was processed, False otherwise.
    """
    create_wallets_table()
    wallets_processed = etl_wallets_by_chain_in_batches(chain_id, batch_size=batch_size)
    return wallets_processed

def etl_wallets_by_chain_in_batches(chain_id, batch_size=500):
    """
    Processes wallets in batches. Returns True if any wallets were processed.
    """
    try:
        log_info(f"Processing wallets for chainId: {chain_id}")
        insert_wallet_query = """
            INSERT INTO wallets (_id, address, chainId, balanceInUSD, tokens, flagged)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (_id) DO NOTHING;
        """

        wallets_cursor = wallets_coll.find(
            {"chainId": chain_id, "balanceInUSD": {"$ne": None}, "tokens": {"$ne": None}},
            limit=batch_size,
        )
        wallets_list = list(wallets_cursor)

        if not wallets_list:  # Return False if no records
            log_info("No wallets found in this batch.")
            return False

        for wallet in wallets_list:
            tokens = json.dumps(wallet.get("tokens", {})) if isinstance(wallet.get("tokens", {}), dict) else None
            try:
                pg_cursor.execute(insert_wallet_query, (
                    wallet["_id"],
                    wallet["address"],
                    wallet["chainId"],
                    wallet.get("balanceInUSD"),
                    tokens,
                    wallet.get("flagged", 0),
                ))
            except Exception as e:
                log_info(f"Error inserting wallet {wallet['_id']}: {e}")

        pg_conn.commit()
        log_info(f"Inserted {len(wallets_list)} wallets in batch.")
        return True

    except Exception as e:
        log_info(f"ETL Wallets by chainId failed: {e}")
        return False
