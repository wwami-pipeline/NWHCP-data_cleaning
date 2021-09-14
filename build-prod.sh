# run on webserver
# need API KEYS
docker run -d -e RED_CAP_API_TOKEN=XXXXXXXXXX -e GOOGLE_MAP_API_TOKEN=XXXXXXXXXX --network host --rm --name nwhcpdatacleaning ghcr.io/wwami-pipeline/nwhcp-data_cleaning:development;
