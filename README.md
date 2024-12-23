# integration_specialist
This is a simple script to pull dealer inventory data from a google sheet
and populate a microsoft SQL database with the scraped data.

## Deployment

For this script to work you must set up a google cloud account and create a cloud project.
Then create a (service account key)[https://cloud.google.com/iam/docs/service-account-creds],
saving it in a file named `credentials.json`.

You must then use your service account email address to get access to
(this google sheet)[https://bit.ly/3DvEUWG]. Then save the
(spreadsheet id)[https://developers.google.com/sheets/api/guides/concepts] to the environment
variable `GOOGLE_SHEET_ID`.

```sh
export GOOGLE_SHEET_ID=<sheet id goes here>
```

Set the environment variables needed by the data parser script:
```sh
export GOOGLE_SHEET_RANGE='A2:Q157'
export DB_USER=sa
export DB_PASSWORD='$Up3rC00lH4ck3rP455w0rd'
export DB_CONTEXT=master
```

Build and deploy the containers:
```sh
docker-compose up --build
```

Clean up when you're done:
```sh
docker-compose down
```

## Development

### Linux Setup
1. Setup ODBC by following instructions [here](https://bit.ly/4gqriKT).

2. create new virtual environment ([see here](https://virtualenvwrapper.readthedocs.io/en/latest/)
   for instructions on installing `virtualenvwraper`):
```sh
mkvirtualenv env1
```

3. Install python packages
```sh
pip install -r ./requirements.txt
```

4. Set the environment variables needed by the data parser script:
```sh
export DB_USER=sa
export DB_PASSWORD=$Up3rC00lH4ck3rP455w0rd
```
5. Build and deploy the sql server container:
```sh
docker-compose up --build -d mssql
```

6. Get the database host environment variable from docker
```sh
export DB_HOST=$(docker inspect mssql -f json | jq -r ".[0].NetworkSettings.Networks.mssqlnet.IPAddress" -)
```

7. Run script:
```sh
chmod +x ./main.py
./main.py
```

8. Clean up when you're done:
```sh
docker-compose down
```
