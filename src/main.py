import datetime
import json
import logging
import os
import time

from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

from utils import import_from_redcap

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    while True:
        # deprecated
        # import_data()
        # import data
        load_data()
        logging.debug(str(datetime.datetime.now()) + " imported data, wait 1hr")
        # repeat
        time.sleep(3600)
