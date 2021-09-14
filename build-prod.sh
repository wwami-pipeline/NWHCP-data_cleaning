# run on webserver
# need API KEYS
docker run -d -e RED_CAP_API_TOKEN=$RED_CAP_API_TOKEN -e GOOGLE_MAP_API_TOKEN=$GOOGLE_MAP_API_TOKEN --network host --pull always --rm --name nwhcpdatacleaning ghcr.io/wwami-pipeline/nwhcp-data_cleaning:development;
