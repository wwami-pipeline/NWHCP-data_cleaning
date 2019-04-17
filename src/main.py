from utils import import_from_redcap
import logging
import schedule
import time
import os
import requests
import json

def start_scheduling():
    try:
        json_to_insert = import_from_redcap.clean_data()
        if len(json_to_insert) > 0:
            # call api to import new data
            logging.info("Get new entires with ID: {0}".format(list(map(lambda x: x.get("_id", 0), json_to_insert))))
            insert_org_api = os.getenv("INSERT_ORG_API", default="http://nwhcp-server/api/v1/pipeline-db/populate-organizations")
            res = requests.post(insert_org_api, json=json.dumps(json_to_insert))

            if res.status_code == 200:
                logging.info("Import from redcap successful")
            else:
                logging.info("Import failed. Bad response from insert API.")
    except Exception as error:
        logging.error("Error cleaning data: " + str(error))

def test():
    logging.warning("test")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    schedule.every(10).seconds.do(start_scheduling)
    # schedule.every().day.at("01:30").do(start_scheduling)
    logging.info("scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(1)
