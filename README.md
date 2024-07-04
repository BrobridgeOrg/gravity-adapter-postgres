# gravity-adapter-postgres

Gravity adapter for PostgreSQL

## config.toml 說明
##### configs/config.toml example
```
[gravity]
domain = "default"
host = "192.168.8.227"
port = 32803
pingInterval = 10
maxPingsOutstanding = 3
maxReconnects = -1
accessToken = ""

[source]
config = "./settings/sources.json"

[store]
enabled = true
path = "./statestore"
```

|參數|說明|
|---|---|
|gravity.domain| 設定gravity domain |
|gravity.host | 設定 gravity 的 nats ip |
|gravity.port | 設定 gravity 的 nats port |
|gravity.pingInterval | 設定 gravity 的 pingInterval |
|gravity.maxPingsOutstanding | 設定 gravity 的 maxPingOutstanding |
|gravity.maxReconnects | 設定 gravity 的 maxReconnects |
|gravity.accessToken | 設定 gravity 的 accessToken (for auth) |
|source.config |設定 Adapter 的 來源設定檔位置 |
|store.enabled |是否掛載 presistent volume (記錄狀態) |
|store.path | 設定 presistent volume 掛載點 (記錄狀態) |


> **INFO**
>
 config.toml 設定可由環境變數帶入，其環境變數命名如下：
 **GRAVITY\_ADAPTER\_POSTGRES** + \_ + **[section]** + \_ + **key**
 其中所有英文字母皆為大寫並使用_做連接。
>
 YAML 由環境變數帶入 gravity.host 範例:
>
```
env:
- name: GRAVITY_ADAPTER_POSTGRES_GRAVITY_HOST
  value: 192.168.0.1
```

## settings.json 說明
##### settings/sources.json example
```
{
	"sources": {
		"my_postgres": {
			"disabled": false,
			"host": "192.168.8.227",
			"port": 5432,
			"username": "postgres",
			"password": "1qaz@WSX",
			"dbname": "gravity",
			"param": "sslmode=disable",
			"initialLoad": true,
			"initialLoadBatchSize": 10000,
			"//_comment_interval": "query interval unit: seconds",
			"interval": 1,
			"slotName": "regression_slot",
			"//_comment_public.account":"schema.tableName",
			"tables": {
				"public.account":{
					"events": {
						"snapshot": "accountInitialized",
						"create": "accountCreated",
						"update": "accountUpdated",
						"delete": "accountDeleted"
					}
				}
			}
		}
	}
}
```

|參數|說明 |
|---|---|
| sources.SOURCE_NAME.disabled | 是否停用這個source |
| sources.SOURCE_NAME.host |設定postgresql server ip |
| sources.SOURCE_NAME.port |設定postgresql server port |
| sources.SOURCE_NAME.username |設定 postgresql 登入帳號 |
| sources.SOURCE_NAME.password |設定 postgresql 登入密碼 |
| sources.SOURCE_NAME.dbname | 設定 postgresql database name |
| sources.SOURCE_NAME.param |  可依照需求加入更多連線參數（例如："sslmode=disable"）可參考 [Connection String Parameters](https://pkg.go.dev/github.com/lib/pq#hdr-Connection_String_Parameters) |
| sources.SOURCE_NAME.initialLoad |  是否同步既有 record （在初始化同步時禁止對該資料表進行操作） |
| sources.SOURCE_NAME.initialLoadBatchSize | 同步既有 record 時 每批次幾筆資料 |
| sources.SOURCE_NAME.interval | InitialLoad Event 的同步間隔 (單位：秒) |
| sources.SOURCE_NAME.slotName | 設定 replication\_slot 名稱 |
| sources.SOURCE_NAME.tables.TABLE\_NAME | 設定要捕獲事件的 table 名稱 格式為 SCHEMA\_NAME.TABLE\_NAME（例如： "public.account"）|
| sources.SOURCE_NAME.tables.TABLE\_NAME.event.snapshot | 設定 initialLoad event name |
| sources.SOURCE_NAME.tables.TABLE\_NAME.event.create | 設定 create event name |
| sources.SOURCE_NAME.tables.TABLE\_NAME.event.update | 設定 update event name |
| sources.SOURCE_NAME.tables.TABLE\_NAME.event.delete | 設定 delete event name |

> **INFO**
>
 資料庫的連線密碼可由環境變數帶入(需要使用工具做 AES 加密)，其環境變數如下：
  **[SOURCE_NAME] + \_ + PASSWORD**
>
>
 settings.json 設定可由環境變數帶入，其環境變數如下：
 **GRAVITY\_ADAPTER\_POSTGRES\_SOURCE\_SETTINGS**
>
 YAML 由環境變數帶入範例:
>
```
env:
- name: GRAVITY_ADAPTER_POSTGRES_SOURCE_SETTINGS
      value: |
        {
            "sources": {
                "my_postgres": {
                    "disabled": false,
                    "host": "192.168.8.227",
                    "port": 5432,
                    "username": "postgres",
                    "password": "1qaz@WSX",
                    "dbname": "gravity",
                    "param": "sslmode=disable",
                    "initialLoad": true,
                    "initialLoadBatchSize": 10000,
                    "//_comment_interval": "query interval unit: seconds",
                    "interval": 1,
                    "slotName": "regression_slot",
                    "//_comment_public.account":"schema.tableName",
                    "tables": {
                        "public.account":{
                            "events": {
                                "snapshot": "accountInitialized",
                                "create": "accountCreated",
                                "update": "accountUpdated",
                                "delete": "accountDeleted"
                            }
                        }
                    }
                }
            }
        }
```

---

> **補充**
>
> 設定 Log 呈現的 Level 可由環境變數帶入:
其設定可使用 **debug**, **info**, **error**
>
```
env:
- name: GRAVITY_DEBUG
  value: debug
```

---
## Build
```
podman buildx build --platform linux/amd64 -t hb.k8sbridge.com/gravity/gravity-adapter-postgres:v2.0.0 -f build/docker/Dockerfile .
```


---

## Enable Database CDC

Setting PostgreSQL

```
vim /var/lib/postgresql/data/postgresql.conf

...
wal_level = logical
max_replication_slots = 1
...
```

max\_replication\_slots 數值設定是根據該 PostgreSQL 會建立幾個 slot 而定，通常 settings 裡有幾個 sources.SOURCE_NAME object 就獨立幾個專用的 slot


Restart PostgreSQL

```
systemctl restart postgresql.service
```

Create a slot named 'regression\_slot' using the output plugin 'test\_decoding'

```
# psql -h localhost -U postgres  -d gravity

postgres=# SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
```

---

## Disable Database CDC
Drop a slot

```
# psql -h localhost -U postgres  -d gravity

postgres=# SELECT pg_drop_replication_slot('regression_slot');
```
 

---

## Create PostgreSQL Account

```
-- 建立帳號並設定密碼
CREATE ROLE gravity WITH LOGIN PASSWORD 'pwdpwdpwd';

-- 給予 REPLICATION 權限
ALTER ROLE gravity WITH REPLICATION;

-- 給予 SELECT 權限
GRANT SELECT ON ALL TABLES IN SCHEMA public TO gravity;

-- 如果有多個 schema，可以重複執行
-- GRANT SELECT ON ALL TABLES IN SCHEMA your_schema TO gravity;

```

---

## Run PostgreSQL using Docker
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

gravity=# create table account(
gravity(#   id  INT primary key,
gravity(#   name  VARCHAR(20),
gravity(#   email VARCHAR(80)                                                             
gravity(# ); 
CREATE TABLE

```

---
## PostgreSQL 版本限制

PostgreSQL 9.4 +

---

## License

Licensed under the MIT License

## Authors

Copyright(c) 2020 Fred Chien <<fred@brobridge.com>>  
Copyright(c) 2020 Jhe Sue <<jhe@brobridge.com>>
