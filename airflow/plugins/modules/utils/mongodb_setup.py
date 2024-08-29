import pymongo, os

# MongoDB connection details
MONGODB_HOST = os.getenv("MONGODB_HOST", "unknown")
MONGODB_PORT =int(os.getenv("MONGODB_PORT"))
MONGODB_USER = os.getenv("MONGODB_USER", "unknown")
MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD", "unknown")

# Database and collection names
db_staging = "staging"
db_caching = "caching"

# Staging collection
inventory_items = "inventory_items"
invoice_details = "invoice_details"
invoices = "invoices"
stocks = "stocks"
supply_goods = "supply_goods"

# Caching collection
cache_amis_config = "amis_config"
cache_invoices = "invoices"


def create_database_if_not_exists(client, db_name):
    if db_name not in client.list_database_names():
        client[db_name].temp.insert_one({'x': 1})

def create_collection_if_not_exists(db, collection_name):
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)

def main():
    try:
        client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT, username=MONGODB_USER, password=MONGODB_PASSWORD)
        create_database_if_not_exists(client, db_staging)
        create_database_if_not_exists(client, db_caching)

        staging = client[db_staging]
        create_collection_if_not_exists(staging, inventory_items)
        create_collection_if_not_exists(staging, invoice_details)
        create_collection_if_not_exists(staging, invoices)
        create_collection_if_not_exists(staging, stocks)
        create_collection_if_not_exists(staging, supply_goods)


        caching = client[db_caching]
        create_collection_if_not_exists(caching, cache_amis_config)
        create_collection_if_not_exists(caching, cache_invoices)

        print("MongoDB setup completed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
