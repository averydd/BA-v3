from models.wallets import run_wallets_etl
from models.transactions import run_transactions_etl
from models.dex_events import run_dex_events_etl
from utils.logger import log_info

def process_etl_cycle(batch_size=500):
    """
    Executes one ETL cycle for:
    - Wallets
    - Transactions
    - DEX events
    Each step processes only one batch in the current cycle.
    """
    # Step 1: Process one batch of wallets
    log_info(f"Processing batch of {batch_size} wallets...")
    wallets_done = run_wallets_etl(batch_size=batch_size)

    # Step 2: Process one batch of transactions
    if wallets_done:
        log_info(f"Processing batch of {batch_size} transactions...")
        transactions_done = run_transactions_etl(batch_size=batch_size)

        # Step 3: Process one batch of dex events
        if transactions_done:
            log_info(f"Processing batch of {batch_size} dex events...")
            run_dex_events_etl(batch_size=batch_size)

if __name__ == "__main__":
    try:
        log_info("Starting ETL process...")
        batch_size = 500  # Define the batch size
        cycle = 1  # Track the current ETL cycle

        while True:
            log_info(f"Starting ETL Cycle {cycle}...")
            
            # Execute one ETL cycle
            process_etl_cycle(batch_size=batch_size)

            log_info(f"ETL Cycle {cycle} completed.")
            cycle += 1  # Increment the cycle counter

    except Exception as e:
        log_info(f"ETL process failed: {e}")
