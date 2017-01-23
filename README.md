[Twitter](https://twitter.com/zensqlmonitor) |
[Email](mailto:sqlzen@hotmail.com)

# influxdb-zabbix
Gather data from Zabbix back-end and send to InfluxDB for enhanced performance

## Getting Started

- InfluxDB: 
	- [Install InfluxDB](https://docs.influxdata.com/influxdb/v1.1/introduction/installation/)
	- [Create database ZABBIX](https://docs.influxdata.com/influxdb/v1.1/introduction/getting_started/) <br />
- Grafana:
	- [Install Grafana](http://docs.grafana.org/installation/)
- influxdb-zabbix:
	- [Install GO](https://golang.org/doc/install)
	- [Setup you GOPATH](https://golang.org/doc/code.html#GOPATH)
	- Run ``` go get github.com/zensqlmonitor/influxdb-zabbix ```
	- Edit the configuration to match your needs  <br />	
- PostgreSQL:
```SQL 
CREATE USER influxdb_zabbix WITH PASSWORD '***';
GRANT USAGE ON SCHEMA public TO influxdb_zabbix;
```
At the database level:
```SQL 
GRANT SELECT ON public.history, public.history_uint TO influxdb_zabbix;
GRANT SELECT ON public.trends, public.trends_uint TO influxdb_zabbix;
```

### How to use GO code

- Run in background: ``` go run influxdb-zabbix.go & ```
- Build in the current directory: ``` go build influxdb-zabbix.go ```
- Install in $GOPATH/bin: ``` go install influxdb-zabbix.go ```

### Dependencies
- Go 1.7
- TOML parser (https://github.com/BurntSushi/toml)
- Pure Go Postgres driver for database/sql (https://github.com/lib/pq/)

## Notes on configuration

- Supported back-end is at this time PostgreSQL only, the most popular back-end for Zabbix. 
- Tables that can be replicated are:
  - history
  - history_uint
  - trends
  - trends_uint
- Tables like history_log, _text and _str are not replicated.
- You can configure a different interval for each table.

## License

MIT-LICENSE. See LICENSE file provided in the repository for details
