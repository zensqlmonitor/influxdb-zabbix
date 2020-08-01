# influxdb-zabbix
Gather data from Zabbix back-end and load to InfluxDB in near real-time for enhanced performance and easier usage with Grafana.

As InfluxDB provides an excellent compression rate (in our case: 7x), this project could be used also to archive Zabbix data.

## Getting Started

- InfluxDB: 
	- [Install InfluxDB](https://docs.influxdata.com/influxdb/latest/introduction/installation/)
	- [Create a database with a retention period ](https://docs.influxdata.com/influxdb/latest/introduction/getting_started/) <br />
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
	GRANT SELECT ON public.applications TO influxdb_zabbix;
	GRANT SELECT ON public.items TO influxdb_zabbix;
	GRANT SELECT ON public.hosts TO influxdb_zabbix;
	GRANT SELECT ON public.hosts_groups TO influxdb_zabbix;
	GRANT SELECT ON public.hstgrp TO influxdb_zabbix;
	GRANT SELECT ON public.items_applications TO influxdb_zabbix;
	```
	
	Create indexes:
	```SQL 
	CREATE UNIQUE INDEX idx_history_clock_ns_itemid
		ON public.history USING btree (clock)
		TABLESPACE zabbixindex;

	CREATE UNIQUE INDEX idx_history_uint_clock_ns_itemid
		ON public.history_uint USING btree (clock)
		TABLESPACE zabbixindex;

	CREATE INDEX idx_trends_clock_itemid
		ON public.trends USING btree (clock)
		TABLESPACE zabbixindex;

	CREATE INDEX idx_trends_uint_clock_itemid
		ON public.trends_uint USING btree (clock)
		TABLESPACE zabbixindex;
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
	GRANT SELECT ON zabbix.applications TO influxdb_zabbix@localhost;
	GRANT SELECT ON zabbix.items TO influxdb_zabbix@localhost;
	GRANT SELECT ON zabbix.hosts TO influxdb_zabbix@localhost;
	GRANT SELECT ON zabbix.hosts_groups TO influxdb_zabbix@localhost;
	GRANT SELECT ON zabbix.hstgrp TO influxdb_zabbix@localhost;
	GRANT SELECT ON zabbix.items_applications TO influxdb_zabbix@localhost;
	
 	flush privileges;
	```

	Create indexes:
	```SQL 
	CREATE UNIQUE INDEX idx_history_clock_ns_itemid
		ON history (clock) USING btree;

	CREATE UNIQUE INDEX idx_history_uint_clock_ns_itemid
		ON history_uint (clock) USING btree;

	CREATE INDEX idx_trends_clock_itemid
		ON trends (clock) USING btree;

	CREATE INDEX idx_trends_uint_clock_itemid
		ON trends_uint (clock) USING btree;
	```
	
	NB:
	For trends_* tables we can use pt-online-schema-change for online index create without lock, but for history_* tables we can
	only use create index, because primary key for these tables does not exist.
	
### How to use GO code

- Run in background: ``` go run influxdb-zabbix.go & ```
- Build in the current directory: ``` go build influxdb-zabbix.go ```
- Install in $GOPATH/bin: ``` go install influxdb-zabbix.go ```

### Goodies
Have a look to the scripts folder

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
