# need API KEYS

docker build -t loibucket/nwhcp-data-cleaning .

docker run -e RED_CAP_API_TOKEN=XXXXXXXXXX -e GOOGLE_MAP_API_TOKEN=XXXXXXXXXX --network host --rm --name nwhcpdatacleaning loibucket/nwhcp-data-cleaning;
