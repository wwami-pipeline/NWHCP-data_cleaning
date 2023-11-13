import json

# Opening JSON file
with open("orgs.json") as json_file:
    data = json.load(json_file)

    # add unique id for mongo
    for o in data["organizations"]:
        o["_id"] = o["OrgId"]

    with open("orgs_unique.json", "w") as f:
        json.dump(data, f)
