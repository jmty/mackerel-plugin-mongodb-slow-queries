# mackerel-plugin-mongodb-slow-queries

This is a custom metric plugin for mackerel-agent for MongoDB.
Visualize the number of slow queries and other operations per minute.

## Preparation

[Database profiler](https://docs.mongodb.com/manual/tutorial/manage-the-database-profiler/index.html) must be on.

Check profiling level with your mongo shell.

```
db.getProfilingStatus()
```

If profiling level is not 1, set profiling level and slow operation threshold. 

```
db.setProfilingLevel(1, { slowms: 50 })
```

## Installation

Install this plugin from [mkr](https://github.com/mackerelio/mkr#installation).

```
mkr plugin install rinmu/mackerel-plugin-mongodb-slow-queries
```

## Synopsis

```shell
mackerel-plugin-mongodb-slow-queries [-metric-key-prefix=<prefix>][-host=<host>] [-port=<port>] [-username=<username>] [-password=<password>] [-database=<database>]
```

```
$ ./mackerel-plugin-mongodb-slow-queries -h
Usage of ./mackerel-plugin-mongodb-slow-queries:
  -database string
        Database name
  -host string
        Hostname (default "localhost")
  -metric-key-prefix string
        Metric key prefix (default "mongodb")
  -password string
        Password
  -port string
        Port (default "27017")
  -username string
        Username
```

## Example of mackerel-agent.conf

```
[plugin.metrics.sample]
command = "/path/to/mackerel-plugin-mongodb-slow-queries -database=your_database_name"
```

## Example

```
$ ./mackerel-plugin-mongodb-slow-queries -database=test
mongodb.slow_queries.count      79      1543970297
```
