from utils import import_from_redcap
import logging
import schedule
import time
import os
import requests

INSERT_ORG_API = os.getenv("INSERT_ORG_API", "http://localhost:4003/api/v1/pipeline-db/poporgs")
TRUNCATE_ORG_API = os.getenv("TRUNCATE_ORG_API", "http://localhost:4003/api/v1/pipeline-db/truncate")


def import_data():
    data = import_from_redcap.get_cleaned_data()
    res = requests.delete(TRUNCATE_ORG_API)
    if res.status_code != 200:
        logging.error("Cannot clean the database for new data")
        return
    res = requests.post(INSERT_ORG_API, json=data)
    logging.info("Import done with status code: " + str(res.status_code))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    import_data()
    # schedule.every(10).seconds.do(start_scheduling)
    schedule.every().day.at("01:30").do(import_data)
    logging.info("scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(1)
