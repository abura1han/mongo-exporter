import os
import json
import multiprocessing
from pymongo import MongoClient

# MongoDB connection details
mongodb_url = "" # Provide DB URL here
database_name = "" # Provide DB name here

# Number of parallel processes
num_processes = multiprocessing.cpu_count()

def export_collection(collection_name, output_dir):
    client = MongoClient(mongodb_url)
    db = client[database_name]
    collection = db[collection_name]
    documents = collection.find()

    output_file = os.path.join(output_dir, f"{collection_name}.json")

    with open(output_file, "w") as f:
        documents_list = list(documents)
        json.dump(documents_list, f, default=str, indent=4)

    print(f"Exported {collection_name}.")

if __name__ == "__main__":
    # Connect to MongoDB
    client = MongoClient(mongodb_url)
    db = client[database_name]

    # Create a directory to store exported JSON files
    output_dir = "exported_collections"
    os.makedirs(output_dir, exist_ok=True)

    # Get a list of all collections in the database
    collections = db.list_collection_names()

    # Create a pool of worker processes
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.starmap(export_collection, [(collection_name, output_dir) for collection_name in collections])

    print("Export completed.")


