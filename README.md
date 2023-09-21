# Service for oders processing
Service which provides interface for oders processing.
there are several public methods:
- `load_data_from_file` - serves as tool for saving the orders data to the database
- `get_orders_in_time_range` - returns the list of orders for a given time range
- `get_top_users_by_product_purchase_count` - returns the list of users with the most product purchase count

### Set up in virtual environment
Best practice is to use the virtual environment for isolate installation of dependencies.
```bash
python3 -m pip install --user virtualenv
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## Usage
If you already have installed all the dependencies, just run the following command:
```bash
python3 main.py -d <database_url> -f <ndjson-file-path>
```
options:
-  `-h`, `--help` - show this help message and exit
-  `-f <ndjson-file-path>`, `--file-path <ndjson-file-path>` - Path to the file to process in ndjson format.
-  `-d <database_url>`, `--database-url <database_url>` - URL to the database.

**Contrete example**
```bash
python3 main.py -d postgresql://postgres:password@localhost:5432/meiro -f data-example.ndjson
```


### Local DB setup
```bash
docker pull postgres
# rename the image
docker tag postgres:latest postgres:meiro
# create volume for container data which ensure persistent data after stopping the container
docker volume create local_meiro
# run the database container with setting the env variables
docker run -d -p 127.0.0.1:5432:5432 -v local_meiro:/var/lib/postgresql/data \
--name meirodb -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password postgres:meiro
# now you have running the database locally with user = postgres and password = password
# check the database be connecting to it
docker exec -it meirodb psql -U postgres
```
```sql
-- now you need to create database inside the container
CREATE database meiro;
```
