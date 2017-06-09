// Package config provides specific configuration options.
package config

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	toml "github.com/BurntSushi/toml"
)

const (
	DefaultRegistryFileName string = "/var/lib/influxdb-zabbix/influxdb-zabbix.json"
	DefaultLogFileName      string = "/var/log/influxdb-zabbix/influxdb-zabbix.log"
	DefaultModes            string = "console"
	DefaultBufferLen        int    = 10000
	DefaultLevelConsole     string = "Trace"
	DefaultLevelFile        string = "Warn"
	DefaultFormatting       bool   = true
	DefaultLogRotate        bool   = true
	DefaultMaxLines         int    = 1000000
	DefaultMaxSizeShift     int    = 28
	DefaultDailyRotate      bool   = true
	DefaultMaxDays          int    = 7

	DefaultPollingInterval        int = 30
	DefaultPollingIntervalIfError int = 60

	DefaultInfluxDBUrl       string = "http://localhost:8086"
	DefaultInfluxDBTimeOut   int    = 0
	DefaultInfluxDBDatabase  string = "zabbix"
	DefaultInfluxDBPrecision string = "ms"

	DefaultZabbixAddress      string = "host=localhost user=zabbix sslmode=disable database=zabbix"
	DefaultTableInterval      int    = 15
	DefaultHoursPerBatch      int    = 320 // 15 days
	DefaultOutputRowsPerBatch int    = 100000
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
	Name               string
	Active             bool
	Interval           int
	Startdate          string
	Hoursperbatch       int
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

var fConfig = flag.String("config",
	"influxdb-zabbix.conf",
	"the configuration file in TOML format")

func Parse(tomlConfig *TOMLConfig) error {
	if _, err := toml.DecodeFile(*fConfig, &tomlConfig); err != nil {
		return err
	}
	return nil
}

func Validate(tomlConfig *TOMLConfig) error {
	if err := tomlConfig.validate(); err != nil {
		return err
	}
	return nil
}

// Validate adds default value, validates the config data
// and returns an error describing any problems or nil.
func (tomlConfig *TOMLConfig) validate() error {

	fmterr := fmt.Errorf

	if tomlConfig.Registry.FileName == "" {
		tomlConfig.Registry.FileName = DefaultRegistryFileName
	}
	if tomlConfig.Logging.FileName == "" {
		tomlConfig.Logging.FileName = DefaultLogFileName
	}
	if tomlConfig.Logging.Modes == "" {
		tomlConfig.Logging.Modes = DefaultModes
	}
	if tomlConfig.Logging.BufferLen == 0 {
		tomlConfig.Logging.BufferLen = DefaultBufferLen
	}
	if tomlConfig.Logging.LevelConsole == "" {
		tomlConfig.Logging.LevelConsole = DefaultLevelConsole
	}
	if tomlConfig.Logging.LevelFile == "" {
		tomlConfig.Logging.LevelFile = DefaultLevelFile
	}
	if tomlConfig.Logging.MaxLines == 0 {
		tomlConfig.Logging.MaxLines = DefaultMaxLines
	}
	if tomlConfig.Logging.MaxSizeShift == 0 {
		tomlConfig.Logging.MaxSizeShift = DefaultMaxSizeShift
	}
	if tomlConfig.Logging.MaxDays == 0 {
		tomlConfig.Logging.MaxDays = DefaultMaxDays
	}
	if tomlConfig.Polling.Interval == 0 {
		tomlConfig.Polling.Interval = DefaultPollingInterval
	}
	if tomlConfig.Polling.IntervalIfError == 0 {
		tomlConfig.Polling.IntervalIfError = DefaultPollingIntervalIfError
	}
	if tomlConfig.InfluxDB.Url == "" {
		tomlConfig.InfluxDB.Url = DefaultInfluxDBUrl
	}
	if tomlConfig.InfluxDB.Database == "" {
		tomlConfig.InfluxDB.Database = DefaultInfluxDBDatabase
	}
	if tomlConfig.InfluxDB.Precision == "" {
		tomlConfig.InfluxDB.Precision = DefaultInfluxDBPrecision
	}
	if tomlConfig.InfluxDB.TimeOut == 0 {
		tomlConfig.InfluxDB.TimeOut = DefaultInfluxDBTimeOut
	}

	// InfluxDB
	fullUrl := strings.Replace(tomlConfig.InfluxDB.Url, "http://", "", -1)

	host, portStr, err := net.SplitHostPort(fullUrl)
	if err != nil {
		return fmterr("Validation failed : InfluxDB url must be formatted as host:port but "+
			"was '%s' (%v).", tomlConfig.InfluxDB.Url, err)
	}
	if len(host) == 0 {
		return fmterr("Validation failed : InfluxDB url value ('%s') is missing a host.",
			tomlConfig.InfluxDB.Url)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmterr("Validation failed : InfluxDB url port value ('%s') must be a number "+
			"(%v).", portStr, err)
	}
	if port < 1 || port > 65535 {
		return fmterr("Validation failed : InfluxDB url port must be within [1-65535] but "+
			"was '%d'.", port)
	}

	// Zabbix
	zabbixes := tomlConfig.Zabbix
	if len(zabbixes) == 0 {
		return fmterr("Validation failed : You must at least define one Zabbix database provider.")
	}
	if len(zabbixes) > 1 {
		return fmterr("Validation failed : You can only define one Zabbix provider.")
	}

	for provider, zabbix := range zabbixes {
		if zabbix.Address == "" {
			return fmterr("Validation failed : You must at least define a Zabbix database address for provider %s.", provider)
		}
		if provider == "mysql" {
			zabbix.Address += "?sql_mode='PIPES_AS_CONCAT'"
		}
	}

	// Zabbix tables
	tables := tomlConfig.Tables
	if len(tables) == 0 {
		return fmterr("Validation failed : You must at least define one table.")
	}
	var activeTablesCount = 0
	for tableName, table := range tables {

		if table.Active {
			activeTablesCount += 1
		}
		if table.Interval < 15 {
			tomlConfig.Tables[tableName].Interval = DefaultTableInterval
		}

		if len(table.Startdate) > 0 {
			// validate date format
			layout := "2006-01-02T15:04:05"
			_, err := time.Parse(
				layout,
				table.Startdate)
			if err != nil {
				return fmterr("Validation failed : Startdate for table %s is not well formatted.", tableName)
			}
		}
		if table.Hoursperbatch == 0 {
			tomlConfig.Tables[tableName].Hoursperbatch = DefaultHoursPerBatch
		}
		if table.Outputrowsperbatch == 0 {
			tomlConfig.Tables[tableName].Outputrowsperbatch = DefaultOutputRowsPerBatch
		}
	}
	if activeTablesCount == 0 {
		return fmterr("Validation failed : You must at least define one active table.")
	}
	return nil
}
