# influxdb-zabbix
Gather data from Zabbix back-end and load to InfluxDB in near real-time for enhanced performance and easier usage with Grafana.

As InfluxDB provides an excellent compression rate (in our case: 7x), this project could be used also to archive Zabbix data.

## Getting Started

- InfluxDB: 
	- [Install InfluxDB](https://docs.influxdata.com/influxdb/v1.1/introduction/installation/)
	- [Create a database with a retention period ](https://docs.influxdata.com/influxdb/v1.1/introduction/getting_started/) <br />
- Grafana:
	- [Install Grafana](http://docs.grafana.org/installation/)
	- [Using InfluxDB in Grafana](http://docs.grafana.org/features/datasources/influxdb/)
- influxdb-zabbix:
	- [Install GO](https://golang.org/doc/install)
	- [Setup you GOPATH](https://golang.org/doc/code.html#GOPATH)
	- Run ``` go get github.com/zensqlmonitor/influxdb-zabbix ```
	- Edit influxdb-zabbix.conf to match your needs  <br />	
- PostgreSQL:

	Create user:
	```SQL 
	CREATE USER influxdb_zabbix WITH PASSWORD '***';
	GRANT USAGE ON SCHEMA public TO influxdb_zabbix;
	```
	Grants at the database level:
	```SQL 
	GRANT SELECT ON public.history, public.history_uint TO influxdb_zabbix;
	GRANT SELECT ON public.trends, public.trends_uint TO influxdb_zabbix;
	```
	
- MariaDB / MySQL:

	Create user:
	```SQL 
	CREATE USER 'influxdb_zabbix'@'localhost' IDENTIFIED BY '***';
	```
	
	Grants at the database level:
	```SQL 
	GRANT SELECT ON zabbix.trends TO influxdb_zabbix@localhost;
	GRANT SELECT ON zabbix.trends_uint TO influxdb_zabbix@localhost;
	GRANT SELECT ON zabbix.history TO influxdb_zabbix@localhost;
	GRANT SELECT ON zabbix.history_uint TO influxdb_zabbix@localhost;
 	flush privileges;
	```


### How to use GO code

- Run in background: ``` go run influxdb-zabbix.go & ```
- Build in the current directory: ``` go build influxdb-zabbix.go ```
- Install in $GOPATH/bin: ``` go install influxdb-zabbix.go ```

### Dependencies
- Go 1.7+
- TOML parser (https://github.com/BurntSushi/toml)
- Pure Go Postgres driver for database/sql (https://github.com/lib/pq/)
- Pure Go MySQL driver for database/sql (https://github.com/go-sql-driver/mysql/)

## Configuration: influxdb-zabbix.conf

- PostgreSQL and MariaDB/MySQL supported.

- Tables that can be replicated are:
  - history
  - history_uint
  - trends
  - trends_uint
- Tables like history_log, _text and _str are not replicated.

- Configurable at table-level:
  - interval: polling interval, minimum of 15 sec
  - hours per batch : number of hours/batch to extract from zabbix backend 
  - output rows per batch :  allow the destination load to be splitted in multiple batches
 
## License

MIT-LICENSE. See LICENSE file provided in the repository for details
