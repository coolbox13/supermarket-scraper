import aiohttp
import asyncio
import json
import os
import logging

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'ALDINord-App-NL/4.23.0 (nl.aldi.aldinordmobileapp; build:2403140920.292755; iOS 17.4.1) Alamofire/5.5.0',
}

class AldiScraper:
    def __init__(self):
        self.base_url = "https://webservice.aldi.nl/api/v1"
        self.output_file = "data/aldi_products.json"
        self.progress_file = "data/aldi_scrape_progress.json"
        self.scraped_products = set()  # Tracks globally scraped product IDs
        self.scraped_categories = set()  # Tracks scraped categories to avoid reprocessing
        self.total_scraped_items = 0
        os.makedirs("data", exist_ok=True)

        # Clear output file at the start of a new scrape
        with open(self.output_file, "w") as f:
            json.dump([], f)  # Start with an empty array

        # Load progress
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r") as f:
                try:
                    progress_data = json.load(f)
                    if isinstance(progress_data, dict):
                        self.scraped_products = set(progress_data.get("scraped_products", []))
                        self.scraped_categories = set(progress_data.get("scraped_categories", []))
                    else:
                        logging.warning("Progress file format is invalid. Resetting progress.")
                except json.JSONDecodeError:
                    logging.warning("Progress file is corrupted. Resetting progress.")

    async def scrape(self):
        """Main scraping method."""
        async with aiohttp.ClientSession() as session:
            categories = await self.fetch_categories(session)

            if not categories:
                logging.error("No categories found.")
                return

            for category in categories.get("productCollections", []):
                await self.scrape_category(session, category)

            self.save_progress()
            logging.info(f"Scraping complete. Total products scraped: {self.total_scraped_items}")

    async def fetch_categories(self, session):
        """Fetch all categories."""
        url = f"{self.base_url}/products.json"
        async with session.get(url, headers=HEADERS) as response:
            if response.status == 200:
                return await response.json()
            logging.error("Failed to fetch categories.")
            return None

    async def scrape_category(self, session, category):
        """Scrape a single category."""
        category_id = category.get("id")
        if not category_id:
            logging.warning("Category missing ID. Skipping.")
            return

        if category_id in self.scraped_categories:
            logging.info(f"Category '{category_id}' already processed. Skipping.")
            return

        logging.info(f"Scraping category: {category_id}")
        products = await self.fetch_products(session, category_id)

        if not products:
            logging.warning(f"No products found for category {category_id}.")
            return

        # Flatten articleGroups to get individual products
        articles = []
        for group in products:
            articles.extend(group.get("articles", []))

        if not articles:
            logging.warning(f"No articles found for category {category_id}.")
            return

        logging.info(f"Category '{category_id}' returned {len(articles)} articles.")
        new_products = [p for p in articles if p.get("articleId") not in self.scraped_products]

        if not new_products:
            logging.warning(f"No new products found for category {category_id}.")
            return

        self.write_products(new_products)
        self.scraped_products.update(p["articleId"] for p in new_products)
        self.scraped_categories.add(category_id)  # Track processed categories
        self.total_scraped_items += len(new_products)

        logging.info(f"Category '{category_id}' scraped: {len(new_products)} new items.")
        logging.info(f"Total unique products so far: {len(self.scraped_products)}")

    async def fetch_products(self, session, category_id):
        """Fetch products for a category."""
        url = f"{self.base_url}/products/{category_id}.json"
        async with session.get(url, headers=HEADERS) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("articleGroups", [])
            logging.error(f"Failed to fetch products for category {category_id}.")
            return None

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
            "scraped_categories": list(self.scraped_categories),
        }
        with open(self.progress_file, "w") as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=4)

# Initialize logging
def initialize_logging(debug_level=logging.INFO):
    logging.basicConfig(
        level=debug_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("aldi_scraper.log"),
            logging.StreamHandler()
        ]
    )

if __name__ == "__main__":
    initialize_logging()
    scraper = AldiScraper()
    asyncio.run(scraper.scrape())
