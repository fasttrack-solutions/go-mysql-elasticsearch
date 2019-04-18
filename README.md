go-mysql-elasticsearch is a service syncing your MySQL data into Elasticsearch automatically.

It uses `mysqldump` to fetch the origin data at first, then syncs data incrementally with binlog.

## Execution flags

|Flag|Env. variable|Default value|Description|
|:----|:----|:---|:---|
|api-port|API_PORT|3000|HTTP API port number|
|brand-id|BRAND_ID|0|Brand ID|
|bulkSize|BULKSIZE|256|Minimal number of items to be inserted in a single bulk|
|bulks-to-track|BULKS_TO_TRACK|100|Bulk requests to keep in time tracker|
|config|CONFIG|./etc/river.toml|go-mysql-elasticsearch config file|
|dataDir|DATADIR|./go-mysql-elasticsearch-data|Path for go-mysql-elasticsearch to save data|
|dataStorage|DATASTORAGE|redis|Data storage (redis/fs)|
|esAddr|ESADDR|127.0.0.1:9200|Elasticsearch addr|
|esHTTPS|ESHTTPS|false|Use HTTPS for ES|
|esPass|ESPASS||Elasticsearch password|
|esUser|ESUSER||Elasticsearch user|
|exec|EXEC|mysqldump|mysqldump execution path|
|flavor|FLAVOR|mysql|Flavor: mysql or mariadb|
|flushBulkTime|FLUSHBULKTIME|200ms|Force flush the pending requests if we don't have enough items >= bulkSize|
|logLevel|LOGLEVEL|Info|log level|
|mappingsDir|MAPPINGSDIR||Mappings directory|
|myAddr|MYADDR|127.0.0.1:3306|MySQL addr|
|myCharset|MYCHARSET|utf8|MySQL DB charset|
|myPass|MYPASS|root|MySQL password|
|myUser|MYUSER|root|MySQL user|
|redisAddr|REDISADDR|127.0.0.1:6379|Redis addr|
|redisDB|REDISDB|0|Redis database|
|redisKeyPostfix|REDISKEYPOSTFIX||Redis key postfix|
|redisPass|REDISPASS||Redis password|
|serverID|SERVERID|1001|MySQL server ID, as a pseudo slave|
|skipMasterData|SKIPMASTERDATA|false|if no privilege to use mysqldump with --master-data, we must skip it|
|skipNoPkTable|SKIPNOPKTABLE|false|Ignore table without primary key|
|statAddr|STATADDR|127.0.0.1:12800|Inner HTTP status address|
|use-single-redis-db|USE_SINGLE_REDIS_DB|false|Use single Redis DB (0), dismiss brand ID in keys if different DBs|

## Install

+ Install Go (1.9+) and set your [GOPATH](https://golang.org/doc/code.html#GOPATH)
+ `go get github.com/fasttrack-solutions/go-mysql-elasticsearch`, it will print some messages in console, skip it. :-)
+ cd `$GOPATH/src/github.com/fasttrack-solutions/go-mysql-elasticsearch`
+ `make`

## How to use?

+ Create table in MySQL.
+ Create the associated Elasticsearch index, document type and mappings if possible, if not, Elasticsearch will create these automatically.
+ Config base, see the example config [river.toml](./etc/river.toml).
+ Set MySQL source in config file, see [Source](#source) below.
+ Customize MySQL and Elasticsearch mapping rule in config file, see [Rule](#rule) below.
+ Start `./bin/go-mysql-elasticsearch -config=./etc/river.toml` and enjoy it.

## Notice

+ MySQL supported version < 8.0
+ ES supported version < 6.0
+ binlog format must be **row**.
+ binlog row image must be **full** for MySQL, you may lost some field data if you update PK data in MySQL with minimal or noblob binlog row image. MariaDB only supports full row image.
+ Can not alter table format at runtime.
+ MySQL table which will be synced should have a PK(primary key), multi columns PK is allowed now, e,g, if the PKs is (a, b), we will use "a:b" as the key. The PK data will be used as "id" in Elasticsearch. And you can also config the id's constituent part with other column.
+ You should create the associated mappings in Elasticsearch first, I don't think using the default mapping is a wise decision, you must know how to search accurately.
+ `mysqldump` must exist in the same node with go-mysql-elasticsearch, if not, go-mysql-elasticsearch will try to sync binlog only.
+ Don't change too many rows at same time in one SQL.

## Source

In go-mysql-elasticsearch, you must decide which tables you want to sync into elasticsearch in the source config.

The format in config file is below:

```
[[source]]
schema = "test"
tables = ["t1", t2]

[[source]]
schema = "test_1"
tables = ["t3", t4]
```

`schema` is the database name, and `tables` includes the table need to be synced.

If you want to sync **all table in database**, you can use **asterisk(\*)**.  
```
[[source]]
schema = "test"
tables = ["*"]

# When using an asterisk, it is not allowed to sync multiple tables
# tables = ["*", "table"]
```

## Rule

By default, go-mysql-elasticsearch will use MySQL table name as the Elasticserach's index and type name, use MySQL table field name as the Elasticserach's field name.  
e.g, if a table named blog, the default index and type in Elasticserach are both named blog, if the table field named title,
the default field name is also named title.

Notice: go-mysql-elasticsearch will use the lower-case name for the ES index and type. E.g, if your table named BLOG, the ES index and type are both named blog.

Rule can let you change this name mapping. Rule format in config file is below:

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"
parent = "parent_id"
id = ["id"]

    [rule.field]
    mysql = "title"
    elastic = "my_title"
```

In the example above, we will use a new index and type both named "t" instead of default "t1", and use "my_title" instead of field name "title".

## Rule field types

In order to map a mysql column on different elasticsearch types you can define the field type as follows:

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"

    [rule.field]
    // This will map column title to elastic search my_title
    title="my_title"

    // This will map column title to elastic search my_title and use array type
    title="my_title,list"

    // This will map column title to elastic search title and use array type
    title=",list"

    // If the created_time field type is "int", and you want to convert it to "date" type in es, you can do it as below
    created_time=",date"
```

Modifier "list" will translates a mysql string field like "a,b,c" on an elastic array type '{"a", "b", "c"}' this is specially useful if you need to use those fields on filtering on elasticsearch.

## Wildcard table

go-mysql-elasticsearch only allows you determind which table to be synced, but sometimes, if you split a big table into multi sub tables, like 1024, table_0000, table_0001, ... table_1023, it is very hard to write rules for every table.

go-mysql-elasticserach supports using wildcard table, e.g:

```
[[source]]
schema = "test"
tables = ["test_river_[0-9]{4}"]

[[rule]]
schema = "test"
table = "test_river_[0-9]{4}"
index = "river"
type = "river"
```

"test_river_[0-9]{4}" is a wildcard table definition, which represents "test_river_0000" to "test_river_9999", at the same time, the table in the rule must be same as it.

At the above example, if you have 1024 sub tables, all tables will be synced into Elasticsearch with index "river" and type "river".

Note: you should [setup relationship](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-parent-field.html) with creating the mapping manually.

## Filter fields

You can use `filter` to sync specified fields, like:

```
[[rule]]
schema = "test"
table = "tfilter"
index = "test"
type = "tfilter"

# Only sync following columns
filter = ["id", "name"]
```

In the above example, we will only sync MySQL table tfiler's columns `id` and `name` to Elasticsearch. 

## Ignore table without a primary key
When you sync table without a primary key, you can see below error message.
```
schema.table must have a PK for a column
```
You can ignore these tables in the configuration like:
```
# Ignore table without a primary key
skip_no_pk_table = true
```

## Elasticsearch Pipeline
You can use [Ingest Node Pipeline](https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest.html) to pre-process documents before indexing, like JSON string decode, merge fileds and more.

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "_doc"

# pipeline id
pipeline = "my-pipeline-id"
```
Node: you should [create pipeline](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html) manually and Elasticsearch >= 5.0.

## Why not other rivers?

Although there are some other MySQL rivers for Elasticsearch, like [elasticsearch-river-jdbc](https://github.com/jprante/elasticsearch-river-jdbc), [elasticsearch-river-mysql](https://github.com/scharron/elasticsearch-river-mysql), I still want to build a new one with Go, why?

+ Customization, I want to decide which table to be synced, the associated index and type name, or even the field name in Elasticsearch.
+ Incremental update with binlog, and can resume from the last sync position when the service starts again.
+ A common sync framework not only for Elasticsearch but also for others, like memcached, redis, etc...
+ Wildcard tables support, we have many sub tables like table_0000 - table_1023, but want use a unique Elasticsearch index and type.

## Todo

+ MySQL 8
+ ES 6
+ Statistic.

## Donate

If you like the project and want to buy me a cola, you can through: 

|PayPal|微信|
|------|---|
|[![](https://www.paypalobjects.com/webstatic/paypalme/images/pp_logo_small.png)](https://paypal.me/siddontang)|[![](https://github.com/siddontang/blog/blob/master/donate/weixin.png)|

## Feedback

go-mysql-elasticsearch is still in development, and we will try to use it in production later. Any feedback is very welcome.

Email: siddontang@gmail.com
