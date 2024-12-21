# integration_specialist
This is a simple script to pull dealer inventory data from a google sheets
and populate a microsoft SQL database with the scraped data.

## Usage

Set the environment variables needed by the data parser script:
```sh
export DB_USER=sa
export DB_PASSWORD=my$uperC00lH4ck3rP455w0rd
```

Build and deploy the containers:
```sh
docker-compose up --build
```

Clean up when you're done:
```sh
docker-compose down
```
