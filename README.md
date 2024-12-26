# integration_specialist
This is a simple python script to pull dealer inventory data from 
(this google sheet)[https://bit.ly/3DvEUWG] and populate a microsoft SQL database
with the scraped data. The data goes through a set of transformations, targeting
either entire rows or individual cells before being written to the database.

The script is built into a docker container and combined with a mssql server via
docker-compose.

## Environment

A `.env` file can be used to set the value of environment variables. The following
environment variable are used by the script:

- `GOOGLE_SHEET_ID`: the uique identifier for the google sheet containing the data.
  For instruction on finding a google sheet id look
  (here)[https://developers.google.com/sheets/api/guides/concepts].

- `GOOGLE_SHEET_RANGE`: defines the cell range on the given google sheet that contains
  the data. (Note: make sure the given range includes all the table columns and rows 
  you would like to include)

- `DEBUG`: if set to `0`, then the script will print update messages to the screen
  before and after each step. Otherwise, nothing will be printed to the screen.

- `SUBNET`: the subnet on which the two docker containers will be. make sure the
  `DB_ADDRESS` value below is on this subnet.

- `DB_ADDRESS`: domain name or ip address of the microsft sql server. Make sure the
  given address is on the given `SUBNET` value.

- `DB_NAME`: the name of the database where the scraped data will be written to.

- `DB_USER`: the user name used to connect to the database.

- `DB_PASSWORD`: the password used to connect to the database.



## Running The Script

### On Local/Development Machine

1. Follow the instructions [here](https://bit.ly/4gqriKT) to setup microsft server ODBC.
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
export SUBNET='192.168.0.0/24'
export DB_ADDRESS='192.168.0.4'
export DB_USER=sa
export DB_NAME=master
export DB_PASSWORD=$Up3rC00lH4ck3rP455w0rd
export GOOGLE_SHEET_ID='not so secret google sheet id'
export GOOGLE_SHEET_RANGE='A2:Q157'
```

5. Build and deploy the sql server container:
```sh
docker-compose up --build -d mssql
```

8. Run script:
```sh
chmod +x ./main.py
./main.py
```

9. Clean up when you're done:
```sh
docker-compose down
```


### In Docker Container

1. Set the environment variables needed by the data parser script:
```sh
export DB_USER=sa
export SUBNET='192.168.0.0/24'
export DB_ADDRESS='192.168.0.4'
export DB_USER=sa
export DB_NAME=master
export DB_PASSWORD=$Up3rC00lH4ck3rP455w0rd
export GOOGLE_SHEET_ID='not so secret google sheet id'
export GOOGLE_SHEET_RANGE='A2:Q157'
``

2. Build and deploy the containers:
```sh
docker-compose up --build
```

3. Clean up when you're done:
```sh
docker-compose down
```

