import aiohttp
import asyncio
import json
import os
import logging
from random import randint

HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    "X-jumbo-store": "national",
}
API_URL = 'https://mobileapi.jumbo.com/v17/search'

class JumboScraper:
    def __init__(self):
        self.output_file = "data/jumbo_products.json"
        self.progress_file = "data/jumbo_scrape_progress.json"
        self.scraped_products = set()
        self.total_scraped_items = 0
        self.limit = 30  # Default page size
        self.last_offset = 0
        os.makedirs("data", exist_ok=True)

        # Load progress
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r") as f:
                progress_data = json.load(f)
                self.scraped_products = set(progress_data.get("scraped_products", []))
                self.last_offset = progress_data.get("last_offset", 0)

        # Clear output file if resuming is not needed
        if self.last_offset == 0:
            with open(self.output_file, "w") as f:
                json.dump([], f)

    async def scrape(self):
        """Main scraping method."""
        async with aiohttp.ClientSession() as session:
            logging.info("Starting Jumbo scraper...")
            offset = self.last_offset

            while True:
                success = await self.scrape_page(session, offset, self.limit)
                if not success:
                    break
                offset += self.limit
                self.last_offset = offset
                self.save_progress()
                await asyncio.sleep(randint(1, 3))  # Random delay to mimic user behavior

            self.save_progress()
            logging.info(f"Scraping complete. Total products scraped: {self.total_scraped_items}")

    async def scrape_page(self, session, offset, limit):
        """Scrape a single page of products."""
        params = {"limit": limit, "offset": offset}
        retries = 3

        while retries > 0:
            try:
                async with session.get(API_URL, headers=HEADERS, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    products = data.get("products", {}).get("data", [])

                    if not products:
                        logging.info(f"No products found at offset {offset}.")
                        return False

                    # Deduplicate and save products
                    new_products = [p for p in products if p.get("id") not in self.scraped_products]
                    self.write_products(new_products)
                    self.scraped_products.update(p["id"] for p in new_products)
                    self.total_scraped_items += len(new_products)

                    logging.info(f"Page {offset // limit + 1}: Scraped {len(new_products)} new items.")
                    return True
            except aiohttp.ClientResponseError as e:
                if e.status == 504:
                    logging.warning(f"Page at offset {offset} timed out. Retrying...")
                    retries -= 1
                    await asyncio.sleep(2)
                else:
                    logging.error(f"Failed to fetch page at offset {offset}: {e}")
                    return False
            except Exception as e:
                logging.error(f"Unexpected error at offset {offset}: {e}")
                return False

    def write_products(self, products):
        """Append products to the output JSON file."""
        if not products:
            return
        with open(self.output_file, "r+") as f:
            existing_data = json.load(f)
            existing_data.extend(products)
            f.seek(0)
            json.dump(existing_data, f, ensure_ascii=False, indent=4)

    def save_progress(self):
        """Save progress to a file."""
        progress_data = {
            "scraped_products": list(self.scraped_products),
            "last_offset": self.last_offset,
        }
        with open(self.progress_file, "w") as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=4)

# Initialize logging
def initialize_logging(debug_level=logging.INFO):
    logging.basicConfig(
        level=debug_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("jumbo_scraper.log"),
            logging.StreamHandler()
        ]
    )

if __name__ == "__main__":
    initialize_logging()
    scraper = JumboScraper()
    asyncio.run(scraper.scrape())
