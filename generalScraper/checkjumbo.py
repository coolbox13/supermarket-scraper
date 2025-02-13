import requests
from math import ceil
from supermarktconnector.errors import PaginationLimitReached
import logging
logger = logging.getLogger('supermarkt_connector')
logger.setLevel(logging.INFO)


class SupermarktConnectorException(Exception):
    pass


class PaginationLimitReached(SupermarktConnectorException):
    pass


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:102.0) Gecko/20100101 Firefox/102.0',
    "X-jumbo-store": "national",
    "X-jumbo-assortmentid": ""
}


class JumboConnector:
    jumbo_api_version = "v17"

    def search_products(self, query=None, page=0, size=30):
        if (page + 1 * size) > 30:
            raise PaginationLimitReached('Pagination limit on Jumbo connector of 30')

        response = requests.get(
            'https://mobileapi.jumbo.com/' + self.jumbo_api_version + '/search',
            headers=HEADERS,
            params={"offset": page * size, "limit": size, "q": query},
        )
       
        
        if not response.ok:
            response.raise_for_status()
        return response.json()

    def search_all_products(self, **kwargs):
        """
        Iterate all the products available, filtering by query or other filters. Will return generator.
        :param kwargs: See params of 'search_products' method, note that size should not be altered to optimize/limit pages
        :return: generator yielding products
        """
        size = kwargs.pop('size', None) or 30
        response = self.search_products(page=0, size=size, **kwargs)
        yield from response['products']['data']

        for page in range(1, ceil(response['products']['total'] / size)):
            try:
                response = self.search_products(page=page, **kwargs)
            except PaginationLimitReached as e:
                logger.warning('Pagination limit reached, capping response: {}'.format(e))
                return
            yield from response['products']['data']

    def get_product_by_barcode(self, barcode):
        response = requests.get(
            'https://mobileapi.jumbo.com/' + self.jumbo_api_version + '/search',
            headers=HEADERS,
            params={"q": barcode},
        )
        if not response.ok:
            response.raise_for_status()
        products = response.json()['products']['data']
        return products[0] if products else None

    def get_product_details(self, product):
        """
        Get advanced details of a product
        :param product: Product ID or raw product object containing ID field
        :return: dict containing product information
        """
        product_id = product if not isinstance(product, dict) else product['id']
        response = requests.get(
            'https://mobileapi.jumbo.com/' + self.jumbo_api_version + '/products/{}'.format(product_id),
            headers=HEADERS
        )
        if not response.ok:
            response.raise_for_status()
        return response.json()

    def get_categories(self):
        response = requests.get(
            'https://mobileapi.jumbo.com/' + self.jumbo_api_version + '/categories',
            headers=HEADERS
        )
        if not response.ok:
            response.raise_for_status()
        return response.json()['categories']['data']

    def get_sub_categories(self, category):
        category_id = category if not isinstance(category, dict) else category['id']
        response = requests.get(
            'https://mobileapi.jumbo.com/' + self.jumbo_api_version + '/categories',
            headers=HEADERS,
            params={"id": category_id}
        )
        if not response.ok:
            response.raise_for_status()
        return response.json()['categories']['data']
    
    def get_all_sub_categories(self, category):
        sub_categories = self.get_sub_categories(category)
        for sub_cat in sub_categories:
            if sub_cat.get('subCategoriesCount', 0) > 0:
                sub_cat['subCategories'] = self.get_all_sub_categories(sub_cat)
        return sub_categories

    def get_all_stores(self):
        response = requests.get(
            'https://mobileapi.jumbo.com/' + self.jumbo_api_version + '/stores',
            headers=HEADERS
        )
        if not response.ok:
            response.raise_for_status()
        return response.json()['stores']['data']

    def get_store(self, store):
        store_id = store if not isinstance(store, dict) else store['id']
        response = requests.get(
            'https://mobileapi.jumbo.com/' + self.jumbo_api_version + '/stores/{}'.format(store_id),
            headers=HEADERS
        )
        if not response.ok:
            response.raise_for_status()
        return response.json()['store']['data']

    def get_all_promotions(self):
        response = requests.get(
            'https://mobileapi.jumbo.com/' + self.jumbo_api_version + '/promotion-overview',
            headers=HEADERS
        )
        if not response.ok:
            response.raise_for_status()
        return response.json()['tabs']

    def get_promotions_store(self, store):
        store_id = store if not isinstance(store, dict) else store['id']
        response = requests.get(
            'https://mobileapi.jumbo.com/' + self.jumbo_api_version + '/promotion-overview',
            headers=HEADERS,
            params={"store_id": store_id}
        )
        if not response.ok:
            response.raise_for_status()
        return response.json()['tabs']


if __name__ == '__main__':
    from pprint import pprint
    connector = JumboConnector()

    # Get all main categories
    categories = connector.get_categories()

    all_products = []

    # Iterate through each main category
    for main_category in categories:
        category_name = main_category["title"]
        category_id = main_category["id"]

        print(f"\n📂 Fetching subcategories for: {category_name} (ID: {category_id})")

        # Get all subcategories
        subcategories = connector.get_sub_categories(main_category)

        for subcategory in subcategories:
            subcategory_name = subcategory["title"]
            print(f"🔍 Searching in subcategory: {subcategory_name}")

            # Fetch all products in the subcategory
            products = list(connector.search_all_products(query=subcategory_name))
            print(f"📦 Found {len(products)} products in {subcategory_name}")

            detailed_products = []

            # Fetch product details
            for product in products:
                try:
                    product_details = connector.get_product_details(product['id'])

                    if not product_details:
                        print(f"⚠️ No details found for product ID {product['id']}, skipping...")
                        continue

                    # Ensure category is included
                    product_details["main_category"] = category_name
                    product_details["subcategory"] = subcategory_name

                    detailed_products.append(product_details)
                    print(f"✅ Added product: {product_details['title']} ({product_details['id']})")

                except Exception as e:
                    print(f"❌ Error fetching details for {product['title']}: {e}")

            # Store results
            all_products.extend(detailed_products)

    # Print final list of all detailed products
    print("\n✅ FINAL PRODUCT LIST:")
    pprint(all_products)
