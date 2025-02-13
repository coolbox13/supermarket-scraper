import os
import sys
import importlib
import multiprocessing
import time
import json
from typing import List, Dict, Any, Optional

# Logging setup
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/scraper_runner.log'
)

# Configuration for input/output directories
BASE_INPUT_DIR = 'data'  # Matches your existing data directory

# Shop-specific configuration
SHOPS = ['ah', 'jumbo', 'aldi', 'plus']

def run_single_scraper(shop: str) -> Optional[List[Dict[str, Any]]]:
    """
    Run a single shop's scraper.
    """
    try:
        # Dynamically import the scraper module
        scraper_module = importlib.import_module(f'{shop}_scraper')
        # Assuming the main scraping function is named `scrape`
        scrape_func = getattr(scraper_module, 'scrape', None)
        if not scrape_func:
            logging.error(f"No 'scrape' function found in {shop}_scraper.py")
            return None

        # Run the scraper
        logging.info(f"Starting {shop.upper()} scraper...")
        start_time = time.time()

        # Call the scraper's main function
        results = asyncio.run(scrape_func())  # Use asyncio.run for async scrapers

        end_time = time.time()
        logging.info(f"{shop.upper()} scraper completed in {end_time - start_time:.2f} seconds")

        # Ensure output directory exists
        os.makedirs(BASE_INPUT_DIR, exist_ok=True)

        # Save results to JSON
        output_path = os.path.join(BASE_INPUT_DIR, f'{shop}_products.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        logging.info(f"Saved {len(results)} products for {shop} to {output_path}")
        return results

    except ImportError:
        logging.error(f"Could not import {shop}_scraper module")
    except Exception as e:
        logging.error(f"Error in {shop} scraper: {e}", exc_info=True)
    return None


def safe_scrape(shop: str, result_dict: Dict):
    """
    Wrapper to run a single shop's scraper and store results.
    """
    shop_results = run_single_scraper(shop)
    if shop_results is not None:
        result_dict[shop] = {
            'total_products': len(shop_results),
            'status': 'success'
        }
    else:
        result_dict[shop] = {
            'total_products': 0,
            'status': 'failed'
        }


def run_scrapers_parallel(shops: List[str]) -> Dict[str, Any]:
    """
    Run scrapers in parallel using multiprocessing.
    """
    manager = multiprocessing.Manager()
    results = manager.dict()

    total_start_time = time.time()

    processes = []
    for shop in shops:
        p = multiprocessing.Process(target=safe_scrape, args=(shop, results))
        p.start()
        processes.append(p)

    # Wait for all processes to complete
    for p in processes:
        p.join()

    total_end_time = time.time()
    logging.info(f"Parallel scraping completed in {total_end_time - total_start_time:.2f} seconds")

    # Convert manager dict to regular dict before returning
    return dict(results)


def main():
    """
    Main entry point for the scraper runner.
    """
    # Parse command-line arguments
    shops_to_scrape = SHOPS  # Default to all shops
    mode = 'parallel'  # Default mode

    # Custom argument parsing
    for arg in sys.argv[1:]:
        if arg.lower() == 'sequential':
            mode = 'sequential'
        elif arg.lower() in SHOPS:
            shops_to_scrape = [arg.lower()]

    # Validate shops
    invalid_shops = set(shops_to_scrape) - set(SHOPS)
    if invalid_shops:
        logging.error(f"Invalid shops specified: {invalid_shops}")
        print(f"Error: Invalid shops {invalid_shops}. Available shops are: {SHOPS}")
        sys.exit(1)

    # Run scrapers
    print(f"Running scrapers in {mode.upper()} mode")
    logging.info(f"Running scrapers in {mode.upper()} mode")

    if mode == 'parallel':
        results = run_scrapers_parallel(shops_to_scrape)
    else:
        raise NotImplementedError("Sequential mode is not implemented yet.")

    # Print summary
    print("\nScraping Summary:")
    for shop, details in results.items():
        print(f"{shop.upper()}: {details['total_products']} products ({details['status']})")

    # Detailed logging
    logging.info("Scraping process completed")
    logging.info("Summary: %s", json.dumps(results, indent=2))


if __name__ == '__main__':
    main()