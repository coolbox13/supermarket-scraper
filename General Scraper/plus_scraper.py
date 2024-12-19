import aiohttp
import asyncio
import requests
import json
import os
import logging

# Define constants
HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
    "Content-Type": "application/json; charset=UTF-8",
    "accept": "application/json",
    "x-csrftoken": "T6C+9iB49TLra4jEsMeSckDMNhQ=",
    "cookie": "SSRT_WA9S=Q-ViZwADAA; SSLB=1; ...",
    "origin": "https://www.plus.nl",
    "referer": "https://www.plus.nl",
}

BASE_URL = "https://www.plus.nl"
CATEGORIES_ENDPOINT = "/screenservices/ECP_Product_CW/Categories/CategoryList_TF/DataActionGetMenuCategories"
PRODUCTS_ENDPOINT = "/screenservices/ECP_Composition_CW/ProductLists/PLP_Content/DataActionGetProductListAndCategoryInfo"
VERSION_ENDPOINT = "/moduleservices/moduleversioninfo"

OUTPUT_DIR = "data"
CATEGORIES_FILE = os.path.join(OUTPUT_DIR, "top_level_categories.json")
ALL_PRODUCTS_FILE = os.path.join(OUTPUT_DIR, "plus_products.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Fetch API version token
async def fetch_version_token(session):
    """Fetch the version token required for subsequent requests."""
    version_url = f"{BASE_URL}{VERSION_ENDPOINT}"
    try:
        async with session.get(version_url, headers=HEADERS) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("versionToken")
    except Exception as e:
        logging.error(f"Failed to fetch version token: {e}")
        return None

# Fetch all categories
async def fetch_categories(session, version_token):
    """Fetch all categories."""
    categories_url = f"{BASE_URL}{CATEGORIES_ENDPOINT}"
    payload = {
        "versionInfo": {"moduleVersion": version_token, "apiVersion": "SpgKBmBbzIq67HF3dBCsXg"},
        "viewName": "MainFlow.ProductListPage",
        "screenData": {"variables": {}},
    }
    try:
        async with session.post(categories_url, headers=HEADERS, json=payload) as response:
            response.raise_for_status()
            data = await response.json()
            return data
    except Exception as e:
        logging.error(f"Failed to fetch categories: {e}")
        return None

# Extract top-level categories
def extract_top_level_categories(raw_data):
    """Extract top-level categories (no ParentName) and exclude 'Promotions'."""
    try:
        categories = json.loads(raw_data["data"]["CategoriesJson"])
        top_level_categories = [
            {
                "Name": category["Category_str"]["Name"],
                "Slug": category["Category_str"]["Slug"],
                "ImageURL": category["Category_str"].get("ImageURL", ""),
            }
            for category in categories
            if "ParentName" not in category["Category_str"] and category["Category_str"]["Slug"] != "0_promotions"
        ]
        return top_level_categories
    except Exception as e:
        logging.error(f"Error extracting top-level categories: {e}")
        return []

# Scrape products for a category
def scrape_category_products(slug):
    """Fetch all products for a category."""
    print(f"Fetching products for category: {slug}...")
    all_products = []
    page_number = 1
    total_pages = 1  # Initial value to enter the loop

    while page_number <= total_pages:
        payload = {
            "versionInfo": {"moduleVersion": "dPDo1Ys8_I6+3zDZC4+jLQ", "apiVersion": "bYh0SIb+kuEKWPesnQKP1A"},
            "viewName": "MainFlow.ProductListPage",
            "screenData": {"variables": {"PageNumber": page_number, "CategorySlug": slug}},
        }
        try:
            response = requests.post(f"{BASE_URL}{PRODUCTS_ENDPOINT}", headers=HEADERS, json=payload)
            response.raise_for_status()
            data = response.json()

            products = data.get("data", {}).get("ProductList", {}).get("List", [])
            total_pages = data.get("data", {}).get("TotalPages", 1)
            all_products.extend(products)

            page_number += 1
        except Exception as e:
            logging.error(f"Failed to fetch products for {slug}, page {page_number}: {e}")
            break

    category_file = os.path.join(OUTPUT_DIR, f"{slug}.json")
    with open(category_file, "w", encoding="utf-8") as f:
        json.dump(all_products, f, indent=4, ensure_ascii=False)
    logging.info(f"Saved {len(all_products)} products for category '{slug}' to {category_file}.")
    return all_products

# Main function to coordinate scraping
async def scrape_plus_data():
    async with aiohttp.ClientSession() as session:
        # Fetch version token
        logging.info("Fetching API version info...")
        version_token = await fetch_version_token(session)
        if not version_token:
            logging.error("Failed to fetch version token. Exiting.")
            return

        # Fetch categories
        logging.info("Fetching categories...")
        raw_data = await fetch_categories(session, version_token)
        if not raw_data:
            logging.error("Failed to fetch categories. Exiting.")
            return

        # Extract and save top-level categories
        top_categories = extract_top_level_categories(raw_data)
        with open(CATEGORIES_FILE, "w", encoding="utf-8") as f:
            json.dump(top_categories, f, indent=4, ensure_ascii=False)
        logging.info(f"Saved top-level categories to {CATEGORIES_FILE}.")

        # Scrape products for each category
        all_products = []
        for category in top_categories:
            slug = category["Slug"]
            category_file = os.path.join(OUTPUT_DIR, f"{slug}.json")

            if not os.path.exists(category_file):
                products = scrape_category_products(slug)
                all_products.extend(products)

        # Save all products to a single file
        with open(ALL_PRODUCTS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_products, f, indent=4, ensure_ascii=False)
        logging.info(f"Saved all products to {ALL_PRODUCTS_FILE}.")

        # Cleanup: Delete individual category files
        for category in top_categories:
            slug = category["Slug"]
            category_file = os.path.join(OUTPUT_DIR, f"{slug}.json")
            if os.path.exists(category_file):
                try:
                    os.remove(category_file)
                    logging.info(f"Deleted file: {category_file}")
                except Exception as e:
                    logging.error(f"Failed to delete file {category_file}: {e}")

# Initialize logging
def initialize_logging():
    """Set up logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("scraper.log"),
            logging.StreamHandler(),
        ],
    )

if __name__ == "__main__":
    initialize_logging()
    asyncio.run(scrape_plus_data())
