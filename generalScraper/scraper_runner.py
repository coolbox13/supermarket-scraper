import os
import sys
import multiprocessing
import subprocess
import time
import json
from typing import List, Dict

# Logging setup
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/scraper_runner.log'
)

# Configuration
BASE_INPUT_DIR = 'data'  # Matches your existing data directory
SCRAPER_DIR = os.path.dirname(os.path.abspath(__file__))  # Directory where scrapers are located
SHOPS = ['ah', 'jumbo', 'aldi', 'plus']  # List of supported scrapers

def run_scraper_process(shop: str, result_dict: Dict):
    """
    Run a scraper script as a standalone subprocess.
    """
    script_path = os.path.join(SCRAPER_DIR, f"{shop}_scraper.py")

    if not os.path.exists(script_path):
        logging.error(f"Scraper script not found: {script_path}")
        result_dict[shop] = {"total_products": 0, "status": "failed", "error": "Script not found"}
        return

    try:
        logging.info(f"🚀 Starting {shop.upper()} scraper...")

        start_time = time.time()
        
        # Run the scraper script as a subprocess
        process = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True
        )
        
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)

        # Capture logs and errors
        if process.returncode == 0:
            logging.info(f"✅ {shop.upper()} scraper completed in {elapsed_time} seconds")
            output_path = os.path.join(BASE_INPUT_DIR, f"{shop}_products.json")

            if os.path.exists(output_path):
                with open(output_path, "r", encoding="utf-8") as f:
                    products = json.load(f)
                    total_products = len(products)
            else:
                total_products = 0

            result_dict[shop] = {"total_products": total_products, "status": "success"}
        else:
            logging.error(f"❌ {shop.upper()} scraper failed. Error:\n{process.stderr}")
            result_dict[shop] = {"total_products": 0, "status": "failed", "error": process.stderr}

    except Exception as e:
        logging.error(f"❌ Error running {shop}_scraper.py: {e}")
        result_dict[shop] = {"total_products": 0, "status": "failed", "error": str(e)}


def run_scrapers_parallel(shops: List[str]) -> Dict[str, Dict]:
    """
    Run scrapers in parallel using multiprocessing.
    """
    manager = multiprocessing.Manager()
    results = manager.dict()

    total_start_time = time.time()
    processes = []

    for shop in shops:
        p = multiprocessing.Process(target=run_scraper_process, args=(shop, results))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    total_end_time = time.time()
    logging.info(f"Parallel scraping completed in {total_end_time - total_start_time:.2f} seconds")

    return dict(results)


def main():
    """
    Main function to execute scrapers in parallel.
    """
    # Default to all shops
    shops_to_scrape = SHOPS  
    mode = "parallel"

    # Handle command-line arguments
    for arg in sys.argv[1:]:
        if arg.lower() == "sequential":
            mode = "sequential"
        elif arg.lower() in SHOPS:
            shops_to_scrape = [arg.lower()]

    invalid_shops = set(shops_to_scrape) - set(SHOPS)
    if invalid_shops:
        logging.error(f"Invalid shops specified: {invalid_shops}")
        print(f"Error: Invalid shops {invalid_shops}. Available shops are: {SHOPS}")
        sys.exit(1)

    print(f"Running scrapers in {mode.upper()} mode")
    logging.info(f"Running scrapers in {mode.upper()} mode")

    if mode == "parallel":
        results = run_scrapers_parallel(shops_to_scrape)
    else:
        raise NotImplementedError("Sequential mode is not implemented yet.")

    print("\nScraping Summary:")
    for shop, details in results.items():
        print(f"{shop.upper()}: {details['total_products']} products ({details['status']})")

    logging.info("Scraping process completed")
    logging.info("Summary: %s", json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
