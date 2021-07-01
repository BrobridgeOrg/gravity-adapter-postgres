# gravity-adapter-postgres

Gravity adapter for PostgreSQL

### Enable Database CDC

Setting PostgreSQL

```
vim /var/lib/postgresql/data/postgresql.conf

...
wal_level = logical
max_replication_slots = 1
...
```

Restart PostgreSQL
```
systemctl restart postgresql.service
```

Create a slot named 'regression_slot' using the output plugin 'test_decoding'
```
# psql -h localhost -U postgres  -d gravity

postgres=# SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
```

---


### Run PostgreSQL using Docker
```
docker run -e "POSTGRES_DB=gravity" \
	-e "POSTGRES_USER=postgres" \
	-e "POSTGRES_PASSWORD=1qaz@WSX" \
	-p 5432:5432 \
	-d \
	postgres:9.4 -c wal_level=logical -c max_replication_slots=1


# Create a slot (regression_slot)

docker exec -it <CONTAINER_ID> bash

psql -h localhost -U postgres  -d gravity

postgres=# SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');

# Create a table

gravity=# create table users(
gravity(#   id  INT primary key,
gravity(#   name  VARCHAR(20),
gravity(#   email VARCHAR(80)                                                             
gravity(# ); 
CREATE TABLE

```
