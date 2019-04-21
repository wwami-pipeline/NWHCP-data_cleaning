from utils import import_from_redcap
import logging
import schedule
import time
import os
import requests
import json

insert_org_api = os.getenv("INSERT_ORG_API", default="http://localhost:4002/api/v1/pipeline-db/poporgs")

def start_scheduling():
    try:
        json_to_insert = import_from_redcap.clean_data()
        if len(json_to_insert) > 0:
            # call api to import new data
            logging.info("Get new entires with ID: {0}".format(list(map(lambda x: x.get("OrgId", -1), json_to_insert))))
            res = requests.post(insert_org_api, json=json_to_insert)

            logging.info("Import from redcap done with status code: " + str(res.status_code) )
           
    except Exception as error:
        logging.error("Error cleaning data: " + str(error))

def import_data_on_start_up():
    with open(import_from_redcap.OUT_PUT_JSON) as f:
        
        data = json.load(f)
        res = requests.post(insert_org_api, json=data)
        logging.info("Import on start up finished with status: "+str(res.status_code))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    import_data_on_start_up()
    # schedule.every(10).seconds.do(start_scheduling)
    schedule.every().day.at("01:30").do(start_scheduling)
    logging.info("scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(1)
