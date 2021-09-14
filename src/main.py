from utils import import_from_redcap
import logging
import schedule
import time
import os
import requests
import json
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern


# read redcap data and import to mongodb
def import_data():
    data = import_from_redcap.get_cleaned_data()

    # write data to mongo-like json file
    with open("redcap.json", "w") as f:
        f.write(json.dumps(data))

    # connect to mongo
    myclient = MongoClient("mongodb://localhost:27017/")

    # database
    db = myclient["mongodb"]
    collection = db["organization"]

    # Loading or Opening the json file
    with open("redcap.json") as file:
        file_data = json.load(file)

    # ignore duplicated data
    collection.with_options(write_concern=WriteConcern(w=0)).insert_many(file_data)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    rotation = 0
    while True:
        # import data
        import_data()
        print("imported data")
        # repeat
        rotation += 1
        if rotation == 10:
            rotation = 0
        time.sleep(10)
