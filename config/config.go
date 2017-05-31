// Package config provides specific configuration options.
package config

import ()

// Defaults for config variables which are not set
const (
	DefaultRegistryFileName string = "influxdb-zabbix.json"
	DefaultLogFileName  	string = "influxdb-zabbix.log"
	DefaultModes        	string = "console"
	DefaultBufferLen    	int    = 10000
	DefaultLevelConsole 	string = "Trace"
	DefaultLevelFile    	string = "Warn"
	DefaultFormatting   	bool   = true
	DefaultLogRotate    	bool   = true
	DefaultMaxLines     	int    = 1000000
	DefaultMaxSizeShift 	int    = 28
	DefaultDailyRotate  	bool   = true
	DefaultMaxDays      	int    = 7

	DefaultPollingInterval        int = 30
	DefaultPollingIntervalIfError int = 60

	DefaultInfluxDBUrl       string = "http://localhost:8086"
	DefaultInfluxDBTimeOut   int    = 0
	DefaultInfluxDBDatabase  string = "zabbix"
	DefaultInfluxDBPrecision string = "ms"

	DefaultZabbixAddress string = "host=localhost user=zabbix sslmode=disable database=zabbix"
)

type TOMLConfig struct {
	InfluxDB influxDB
	Zabbix   map[string]*zabbix
	Tables   map[string]*Table
	Polling  polling
	Logging  logging
	Registry registry
}

type influxDB struct {
	Url       string
	Database  string
	Username  string
	Password  string
	Precision string
	TimeOut   int
}

type zabbix struct {
	Address string
}

type Table struct {
	Name     string
	Active   bool
	Interval int
	Startdate string
	Inputrowsperbatch int
	Outputrowsperbatch int
}
type registry struct {
	FileName string       
}
type polling struct {
	Interval        int
	IntervalIfError int
}
type logging struct {
	Modes        string
	BufferLen    int
	LevelConsole string
	LevelFile    string
	FileName     string
	Formatting   bool
	LogRotate    bool
	MaxLines     int
	MaxSizeShift int
	DailyRotate  bool
	MaxDays      int
}
