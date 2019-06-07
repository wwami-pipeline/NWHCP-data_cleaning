# NWHCP-data_cleaning

## Intro
This service will import and clean data from RedCap.

This program will run itself on container up, and every day at 1:30 utc so that all data is same as in RedCap.

It firstly clears the db, then inserts new data.

## IMPORTANT!

The redcap API key in this repo is deprecated, there is a new one in technical documentation. **DO NOT** push it here, or anyone with the key will have access to our red cap data (even deleting them all).

## Set up for local dev

- Have all your backend services running, DB and server, or it will have nothing to call and exit itself.
- You can turn to NWHCP-docker to use docker-compose to start everything at once.


