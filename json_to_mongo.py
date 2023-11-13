import json
from pymongo import MongoClient

# connect to mongo
myclient = MongoClient("mongodb://localhost:27017/")

# database
db = myclient["mongodb"]

# Created or Switched to collection
# names: GeeksForGeeks
collection = db["organization"]

# Loading or Opening the json file
with open("orgs_unique.json") as file:
    file_data = json.load(file)

collection.insert_many(file_data["organizations"])
