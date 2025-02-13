import aiohttp
import asyncio
import json
import os
import logging
from random import uniform
from urllib.parse import urlencode, quote

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:102.0) Gecko/20100101 Firefox/102.0',
    "X-jumbo-store": "national",
}

class EnhancedJumboScraper:
    def __init__(self):
        self.base_url = 'https://mobileapi.jumbo.com/v17'
        self.output_dir = "data"
        self.products_file = f"{self.output_dir}/jumbo_products.json"
        self.progress_file = f"{self.output_dir}/jumbo_scrape_progress.json"
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize progress tracking
        self.scraped_products = set()
        self.load_progress()

    def load_progress(self):
        """Load previous scraping progress if it exists."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    self.scraped_products = set(progress.get('scraped_products', []))
                logging.info(f"📂 Loaded progress: {len(self.scraped_products)} products already scraped")
            except json.JSONDecodeError:
                logging.warning("⚠️ Progress file corrupted, starting fresh")

    def save_progress(self):
        """Save current scraping progress."""
        with open(self.progress_file, 'w') as f:
            json.dump({'scraped_products': list(self.scraped_products)}, f, indent=4)

    def save_products(self, products):
        """Save or update products in the output file."""
        existing_products = []
        if os.path.exists(self.products_file):
            with open(self.products_file, 'r') as f:
                try:
                    existing_products = json.load(f)
                except json.JSONDecodeError:
                    logging.warning("⚠️ Products file corrupted, starting fresh")

        # Add new products
        existing_products.extend(products)
        
        # Save updated product list
        with open(self.products_file, 'w') as f:
            json.dump(existing_products, f, indent=4, ensure_ascii=False)

    async def get_categories(self, session):
        """Fetch all categories."""
        url = f"{self.base_url}/categories"
        async with session.get(url, headers=HEADERS) as response:
            response.raise_for_status()
            data = await response.json()
            return data['categories']['data']

    async def scrape_category(self, session, category):
        """Scrape products for a specific category using filters=category:<id>."""
        offset = 0
        limit = 30
        total_scraped = 0

        while True:
            params = {
                'offset': offset,
                'limit': limit,
                'sort': '',
                'filters': f'category:{category["id"].replace("category:", "")}',  # ✅ Fix: Remove extra "category:"
                'current_url': quote(category.get('title', 'Unknown Category'), safe='')  # ✅ Fix: Force `%20` instead of `+`
            }
            
            url = f"{self.base_url}/search"
            full_url = f"{url}?{urlencode(params, safe=':,')}"  # ✅ Fix: Correct encoding

            logging.info(f"📡 Full GET Request: {full_url}")

            async with session.get(url, headers=HEADERS, params=params) as response:
                response_text = await response.text()

                try:
                    response.raise_for_status()
                    search_results = await response.json()
                except Exception as e:
                    logging.error(f"❌ Error fetching category {category['title']}: {e}")
                    logging.error(f"❌ Response text: {response_text}")
                    return 0

                logging.info(f"🔍 Response Keys: {search_results.keys()}")

                products = search_results.get('products', {}).get('data', [])
                if not products:
                    logging.warning(f"⚠️ No products found for category {category['title']}")
                    break

                detailed_products = []
                for product in products:
                    product_id = product.get('id')
                    if product_id and product_id not in self.scraped_products:
                        product_entry = {
                            'product': product,
                            'mainCategory': category.get('title', 'Unknown Category')  # ✅ Store only the main category
                        }
                        detailed_products.append(product_entry)
                        self.scraped_products.add(product_id)
                        total_scraped += 1
                        logging.info(f"✅ Scraped: {product.get('title', 'Unknown Title')} - Category: {category.get('title', 'Unknown')}")

                if detailed_products:
                    self.save_products(detailed_products)
                    self.save_progress()

                offset += len(products)
                await asyncio.sleep(0.2)
        
        logging.info(f"✅ Finished {category['title']}: {total_scraped} products")
        return total_scraped


    async def scrape_all_products(self, session):
        """Scrape all products across all categories."""
        categories = await self.get_categories(session)
        logging.info(f"📂 Found {len(categories)} main categories")

        total_processed = 0
        for category in categories:
            logging.info(f"🔍 Processing category: {category['title']}")
            total_processed += await self.scrape_category(session, category)
            
        logging.info(f"✅ Total products processed: {total_processed}")

    async def scrape(self):
        """Main scraping method."""
        async with aiohttp.ClientSession() as session:
            logging.info("🚀 Starting enhanced Jumbo scraper...")
            await self.scrape_all_products(session)
            logging.info("✅ Scraping completed!")

def main():
    logging.info("🟢 Enhanced Jumbo Scraper Started")
    scraper = EnhancedJumboScraper()
    asyncio.run(scraper.scrape())

if __name__ == "__main__":
    main()
