import aiohttp
import asyncio
import json
import os
import logging

HEADERS = {
    'Host': 'api.ah.nl',
    'x-application': 'AHWEBSHOP',
    'user-agent': 'AHBot/1.0',
    'content-type': 'application/json; charset=UTF-8',
}

class AHConnector:
    def __init__(self):
        self.base_url = "https://api.ah.nl/mobile-services"
        self.auth_url = "https://api.ah.nl/mobile-auth/v1/auth/token/anonymous"
        self.access_token = None

    async def authenticate(self, session):
        """Fetch an anonymous access token."""
        payload = {"clientId": "appie"}
        async with session.post(self.auth_url, headers=HEADERS, json=payload) as response:
            response.raise_for_status()
            self.access_token = (await response.json()).get("access_token")
            logging.info("Authenticated successfully.")

    async def get_categories(self, session):
        """Fetch main categories."""
        url = f"{self.base_url}/v1/product-shelves/categories"
        headers = {**HEADERS, "Authorization": f"Bearer {self.access_token}"}
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.json()

    async def get_sub_categories(self, session, category):
        """Fetch subcategories for a given category."""
        category_id = category['id']
        url = f"{self.base_url}/v1/product-shelves/categories/{category_id}/sub-categories"
        headers = {**HEADERS, "Authorization": f"Bearer {self.access_token}"}
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.json()

    async def search_products(self, session, query, page=0, size=750):
        """Fetch products by category with pagination."""
        url = f"{self.base_url}/product/search/v2"
        headers = {**HEADERS, "Authorization": f"Bearer {self.access_token}"}
        params = {"query": query, "page": page, "size": size}
        async with session.get(url, headers=headers, params=params) as response:
            response.raise_for_status()
            return await response.json()

class AHScraper:
    def __init__(self):
        self.connector = AHConnector()
        self.output_file = "data/ah_products.json"
        self.progress_file = "data/ah_scrape_progress.json"
        self.scraped_ids = set()
        self.total_scraped_items = 0  # Track the total number of scraped items
        os.makedirs("data", exist_ok=True)

        # Load progress
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r") as f:
                self.scraped_ids = set(json.load(f))

    async def scrape(self):
        """Main scraping method."""
        async with aiohttp.ClientSession() as session:
            await self.connector.authenticate(session)

            categories = await self.connector.get_categories(session)
            all_products = []

            for category in categories:
                subcategories = await self.connector.get_sub_categories(session, category)
                for subcategory in subcategories.get('children', []):
                    await self.scrape_subcategory(session, subcategory, all_products)

            self.save_progress()
            logging.info(f"Scraping complete. Total products scraped: {self.total_scraped_items}")

    async def scrape_subcategory(self, session, subcategory, all_products):
        """Scrape a single subcategory."""
        subcategory_name = subcategory.get('name')
        if subcategory_name in self.scraped_ids:
            logging.info(f"Subcategory '{subcategory_name}' already scraped. Skipping.")
            return

        logging.info(f"Scraping subcategory: {subcategory_name}")
        page = 0
        subcategory_scraped_items = 0  # Track items for this subcategory

        while True:
            data = await self.connector.search_products(session, query=subcategory_name, page=page)
            products = data.get("products", [])
            if not products:
                break

            # Deduplicate and save products
            new_products = [p for p in products if p["webshopId"] not in self.scraped_ids]
            all_products.extend(new_products)
            self.write_products(new_products)

            # Update scraped IDs
            self.scraped_ids.update(p["webshopId"] for p in new_products)
            subcategory_scraped_items += len(new_products)
            self.total_scraped_items += len(new_products)

            page += 1
            await asyncio.sleep(0.5)

        self.scraped_ids.add(subcategory_name)
        logging.info(f"Subcategory '{subcategory_name}' scraped: {subcategory_scraped_items} items.")

    def write_products(self, products):
        """Append products to the output JSON file."""
        if not os.path.exists(self.output_file):
            with open(self.output_file, "w") as f:
                json.dump([], f)

        with open(self.output_file, "r+") as f:
            existing_data = json.load(f)
            existing_data.extend(products)
            f.seek(0)
            json.dump(existing_data, f, ensure_ascii=False, indent=4)

    def save_progress(self):
        """Save progress to a file."""
        with open(self.progress_file, "w") as f:
            json.dump(list(self.scraped_ids), f)

# Initialize logging
def initialize_logging(debug_level=logging.INFO):
    logging.basicConfig(
        level=debug_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("ah_scraper.log"),
            logging.StreamHandler()
        ]
    )

if __name__ == "__main__":
    initialize_logging()
    scraper = AHScraper()
    asyncio.run(scraper.scrape())
