#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import googlemaps
import requests
import os
import json
import logging

OUT_PUT_FILE_PATH = "src/output/pipelinesurveydata.csv"
GEO_CODE_JSON_PATH = "src/output/geocode.json"

RED_CAP_TOKEN = os.getenv("RED_CAP_API_TOKEN")
GOOGLE_MAP_TOKEN = os.getenv("GOOGLE_MAP_API_TOKEN")
gmaps = googlemaps.Client(key=GOOGLE_MAP_TOKEN)


# read from redcap api, save raw data to csv
def get_csv():
    data = {"token": RED_CAP_TOKEN, "content": "record", "format": "csv", "filterLogic": "[pipeline_mapping_project_survey_complete]=2"}

    headers = {
        # 'content-type': 'application/json',
        "content-type": "application/x-www-form-urlencoded",
    }

    res = requests.post("https://redcap.iths.org/api/", data=data, headers=headers)
    if res.status_code != 200:
        raise Exception("request error")

    with open(OUT_PUT_FILE_PATH, "w") as f:
        f.write(str(res.text))


# get latitude and longitude of address
def get_lat_lng(full_address: str):
    if full_address == "":
        return {"lat": 0, "lng": 0}

    if not os.path.exists(GEO_CODE_JSON_PATH):
        f = open(GEO_CODE_JSON_PATH, "w+")
        f.write("{}")
        f.close()

    geo_code_dict = {}
    with open(GEO_CODE_JSON_PATH) as fp:
        try:
            geo_code_dict = json.load(fp)
        except:
            geo_code_dict = {}

    if geo_code_dict.get(full_address, None) is None:
        result = gmaps.geocode(full_address)
        lat, lng = 0, 0
        if len(result) > 0:
            lat = result[0].get("geometry", {}).get("location", {}).get("lat", 0)
            lng = result[0].get("geometry", {}).get("location", {}).get("lng", 0)
        geo_code_dict[full_address] = {"lat": lat, "lng": lng}

        with open(GEO_CODE_JSON_PATH, "w+") as fp:
            fp.write(json.dumps(geo_code_dict, indent=4))

        return {"lat": lat, "lng": lng}
    else:
        return geo_code_dict[full_address]


# read raw csv clean data to list of dict
def get_cleaned_data():

    try:
        get_csv()
    except error:
        logging.error("Cannot pull data from red cap. Is API key correct?")
        return
    else:
        # logging.info(ids_already_in_db)
        csv_input = pd.read_csv(OUT_PUT_FILE_PATH, encoding="ISO-8859-1")

        csv_input["Full Address"] = csv_input["street_address_1"] + ", " + csv_input["org_city"] + ", " + csv_input["org_state"] + " " + csv_input["zip_code"]

        j = []
        csv_input = csv_input.fillna("")
        for index, row in csv_input.iterrows():

            org = {}
            geocode = get_lat_lng(row["Full Address"])
            if geocode["lat"] == 0 and geocode["lng"] == 0:
                continue  # skip if no location
            org["_id"] = row["participant_id"]
            org["OrgId"] = row["participant_id"]
            org["OrgTitle"] = row["org_name"]
            org["OrgWebsite"] = row["org_website"]
            org["StreetAddress"] = row["street_address_1"]
            org["City"] = row["org_city"]
            org["State"] = row["org_state"]
            org["ZipCode"] = row["zip_code"]
            org["Phone"] = row["org_phone_number"]
            org["Email"] = row["org_email"]
            org["ActivityDesc"] = row["description"]
            org["Lat"] = geocode["lat"]
            org["Long"] = geocode["lng"]
            org["HasShadow"] = bool(row["has_shadow"] == 1)
            org["HasCost"] = bool(row["has_cost"] == 1)
            org["HasTransport"] = bool(row["provides_transportation"] == 1)
            org["Under18"] = bool(row["age_requirement___under_18"] == 1)
            careers = []
            if row["career_emp___medicine"] == 1:
                careers.append("Medicine")
            if row["career_emp___nursing"] == 1:
                careers.append("Nursing")
            if row["career_emp___dentistry"] == 1:
                careers.append("Dentistry")
            if row["career_emp___pharmacy"] == 1:
                careers.append("Pharmacy")
            if row["career_emp___social_work"] == 1:
                careers.append("Social Work")
            if row["career_emp___public_health"] == 1:
                careers.append("Public Health")
            if row["career_emp___gen_health_sci"] == 1:
                careers.append("Generic Health Sciences")
            if row["career_emp___allied_health"] == 1:
                careers.append("Allied Health")
            if row["career_emp___stem"] == 1:
                careers.append("STEM")
            if len(row["career_emp_other"]) > 0:
                careers.append(row["career_emp_other"])
            org["CareerEmp"] = careers
            gradeLevels = []
            if row["target_school_age___middle"] == 1:
                gradeLevels.append(0)
            if row["target_school_age___highschool"] == 1:
                gradeLevels.append(1)
            if row["target_school_age___com_college"] == 1:
                gradeLevels.append(2)
            if row["target_school_age___undergrad"] == 1:
                gradeLevels.append(3)
            if row["target_school_age___postgrad"] == 1:
                gradeLevels.append(4)
            if row["target_school_age___other"] == 1:
                gradeLevels.append(5)
            org["GradeLevels"] = gradeLevels
            j.append(org)

        return j
