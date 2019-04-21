#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import googlemaps
import requests
import os
import logging
import json

OUT_PUT_FILE = "src/output/pipelinesurveydata.csv"
OUT_PUT_JSON = "src/output/data.json"

def get_csv():
    token = os.getenv("RED_CAP_API_TOKEN", default="2E036BFA999B6FB594112D27F524EA92")
    data = {
        "token": token,
        "content": "record",
        "format": "csv",
        "filterLogic": "[pipeline_mapping_project_survey_complete]=2"
    }

    headers = {
        # 'content-type': 'application/json',
        'content-type': 'application/x-www-form-urlencoded',
    }

    res = requests.post("https://redcap.iths.org/api/", data=data, headers=headers)
    if res.status_code != 200:
        raise Exception("")

    with open(OUT_PUT_FILE, "w") as f:
        f.write(str(res.text))


def clean_data():
    get_csv()
    get_all_org_api = os.getenv("GET_ALL_ORG_API", default="http://localhost:4002/api/v1/orgs")
    google_map_api_tk= os.getenv("GOOGLE_MAP_API_TOKEN", default="AIzaSyDp-LsNg9RusqlMLx2K9_VXXWudUk2-d6c")

    data_in_db = requests.get(get_all_org_api).json()
    ids_already_in_db = list(map(lambda x: x.get("OrgId"), data_in_db))
    # logging.info(ids_already_in_db)
    gmaps = googlemaps.Client(key=google_map_api_tk)

    csv_input = pd.read_csv(OUT_PUT_FILE, encoding="ISO-8859-1")

    csv_input['Full Address'] = csv_input['street_address_1'] + ", " + \
                                csv_input['org_city'] + ", " + \
                                csv_input['org_state'] + " " + csv_input['zip_code']

    j = []
    csv_input = csv_input.fillna('')
    for index, row in csv_input.iterrows():
        if row['participant_id'] in ids_already_in_db:
            # print("Participant with id {0} already exists in database".format(row['participant_id']))
            continue
        org = {}
        result = gmaps.geocode(row['Full Address'])
        org['OrgId'] = row['participant_id']
        org['OrgTitle'] = row['org_name']
        org['OrgWebsite'] = row['org_website']
        org['StreetAddress'] = row['street_address_1']
        org['City'] = row['org_city']
        org['State'] = row['org_state']
        org['ZipCode'] = row['zip_code']
        org['Phone'] = row['org_phone_number']
        org['Email'] = row['org_email']
        org['ActivityDesc'] = row['description']
        org['Lat'] = result[0]['geometry']['location']['lat']
        org['Long'] = result[0]['geometry']['location']['lng']
        org['HasShadow'] = bool(row['has_shadow'] == 1)
        org['HasCost'] = bool(row['has_cost'] == 1)
        org['HasTransport'] = bool(row['provides_transportation'] == 1)
        org['Under18'] = bool(row['age_requirement___under_18'] == 1)
        careers = []
        if row['career_emp___medicine'] == 1:
            careers.append("Medicine")
        if row['career_emp___nursing'] == 1:
            careers.append("Nursing")
        if row['career_emp___dentistry'] == 1:
            careers.append("Dentistry")
        if row['career_emp___pharmacy'] == 1:
            careers.append("Pharmacy")
        if row['career_emp___social_work'] == 1:
            careers.append("Social Work")
        if row['career_emp___public_health'] == 1:
            careers.append("Public Health")
        if row['career_emp___gen_health_sci'] == 1:
            careers.append("Generic Health Sciences")
        if row['career_emp___allied_health'] == 1:
            careers.append("Allied Health")
        if row['career_emp___stem'] == 1:
            careers.append("STEM")
        if (len(row['career_emp_other']) > 0):
            careers.append(row['career_emp_other'])
        org['CareerEmp'] = careers
        gradeLevels = []
        if row['target_school_age___middle'] == 1:
            gradeLevels.extend((6, 7, 8))
        if row['target_school_age___highschool'] == 1:
            gradeLevels.extend((9, 10, 11, 12))
        org['GradeLevels'] = gradeLevels
        j.append(org)
    
    return j
