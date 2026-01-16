import datetime
import json
import logging
import os
import time

from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

from utils import import_from_redcap
from transform import load_mapping, normalize_mapping, load_input, transform_many, write_to_mongo

OUT_PUT_JSON_PATH = "src/output/pipelinesurveydata.json"
MONGO_ADDR = os.environ.get("MONGO_ADDR", "mongodb://localhost:27017/")


def import_data():
    """
    DEPRECATED
    read redcap data and import to mongodb
    """

    data = import_from_redcap.get_cleaned_data()

    # write data to mongo-like json file
    with open("redcap.json", "w") as f:
        f.write(json.dumps(data))

    # connect to mongo
    myclient = MongoClient(MONGO_ADDR)

    # database
    db = myclient["mongodb"]
    collection = db["organization"]

    # Loading or Opening the json file
    with open("redcap.json") as file:
        file_data = json.load(file)

    # ignore duplicated data
    collection.with_options(write_concern=WriteConcern(w=0)).insert_many(file_data)


def load_data():
    """
    read redcap data and import to mongodb
    """

    import_from_redcap.get_json()

    # connect to mongo
    myclient = MongoClient(MONGO_ADDR)

    # database
    db = myclient["mongodb"]
    collection = db["surveys"]

    # Loading or Opening the json file
    with open(OUT_PUT_JSON_PATH) as file:
        file_data = json.load(file)

    # delete data and replace with new data
    collection.delete_many({})
    collection.with_options(write_concern=WriteConcern(w=0)).insert_many(file_data)


def transform_data():
    """
    transform raw survey data to canonical format and upsert to mongodb
    """
    try:
        # Load and normalize mapping
        mapping_raw = load_mapping()
        mapping = normalize_mapping(mapping_raw)

        # Load input records
        records = load_input()

        # Transform records
        transformed = transform_many(records, mapping)

        # Write to MongoDB
        write_to_mongo(transformed, MONGO_ADDR)

        logging.info(f"Transformed and upserted {len(transformed)} records to organization collection")

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        # Don't crash the pipeline - continue with next import cycle


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    while True:
        # deprecated
        # import_data()
        # import data
        load_data()
        # transform data to canonical format
        transform_data()
        logging.debug(str(datetime.datetime.now()) + " imported and transformed data, wait 1hr")
        # repeat
        time.sleep(3600)
