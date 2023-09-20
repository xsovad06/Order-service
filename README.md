# Service for oders processing

## Usage
```bash
python3 main.py <ndjson-file-path>
```
- `<ndjson-file-path>` represents the the path to the oders file in ndjson format

### Set up in virtual environment
```bash
source env/bin/activate
```

### Local DB setup
```bash
docker pull postgres
# rename the image
docker tag postgres:latest postgres:meiro
# create volume for container data
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
