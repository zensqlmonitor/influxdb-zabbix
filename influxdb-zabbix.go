package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	cfg "github.com/zensqlmonitor/influxdb-zabbix/config"
	helpers "github.com/zensqlmonitor/influxdb-zabbix/helpers"
	input "github.com/zensqlmonitor/influxdb-zabbix/input"
	log "github.com/zensqlmonitor/influxdb-zabbix/log"
	influx "github.com/zensqlmonitor/influxdb-zabbix/output/influxdb"
	registry "github.com/zensqlmonitor/influxdb-zabbix/reg"
)

var exitChan = make(chan int)

var wg sync.WaitGroup

type TOMLConfig cfg.TOMLConfig

var config cfg.TOMLConfig

type DynMap map[string]interface{}

type Param struct {
	input  Input
	output Output
}

type Input struct {
	provider     string
	address      string
	tablename    string
	interval     int
	hoursperbatch int
}

type Output struct {
	address            string
	database           string
	username           string
	password           string
	precision          string
	outputrowsperbatch int
}

type InfluxDB struct {
	output Output
}

var mapTables = make(registry.MapTable)

//
// Gather data
//
func (p *Param) gatherData() error {

	 var infoLogs []string
		
	//start watcher
	startwatch := time.Now()

	// read registry, init if not exists
	if err := registry.Read(&config, &mapTables); err != nil {
		fmt.Println(err)
		return err
	}

	// set time
	starttimereg := registry.GetValueFromKey(
		mapTables, p.input.tablename)

	// no start date configured ? return
	if len(starttimereg) == 0 {
		log.Fatal(1,
			"No start time defined for table %s",
			p.input.tablename)
		return nil
	}

	// format start date
	startimerfc, err := time.Parse("2006-01-02T15:04:05", starttimereg)
	if err != nil {
		startimerfc, err = time.Parse(time.RFC3339, starttimereg)
		if err != nil {
			return err
		}
	}

	// add days
	var starttimestr string = strconv.FormatInt(startimerfc.Unix(), 10)
	//var endtimestr string = strconv.FormatInt(startimerfc.AddDate(0, 0, p.input.daysperbatch).Unix(), 10)
	var endtimetmp time.Time = startimerfc.Add(time.Hour * time.Duration(p.input.hoursperbatch))
	var endtimestr string = strconv.FormatInt(endtimetmp.Unix(), 10)
	
	// first time, instantiated with the start time value stored in registry
	var maxclock time.Time = startimerfc
	
	// <--  Extract
	ext := input.NewExtracter(
		p.input.provider,
		p.input.address,
		p.input.tablename,
		starttimestr,
		endtimestr)

	var tlen int = len(p.input.tablename)

	infoLogs = append(infoLogs, 
		fmt.Sprintf(
			"----------- | %s | [%v --> %v[",
			helpers.RightPad(p.input.tablename, " ", 19-tlen),
			startimerfc.Format("2006-01-02 15:04"),
			endtimetmp.Format("2006-01-02 15:04")))

	if err := ext.Extract(); err != nil {
		log.Error(1, "Error while executing script: %s", err)
		return err
	}

	var rowcount int = len(ext.Result)
	if ext.Maxclock.IsZero() == false {
		maxclock = ext.Maxclock
	}
	
	infoLogs = append(infoLogs, 
		fmt.Sprintf(
			"<-- Extract | %s| %v rows | took %s",
			helpers.RightPad(p.input.tablename, " ", 20-tlen),
			rowcount,
			time.Since(startwatch)))

	/// no row, save in registry
	if rowcount == 0 {
		infoLogs = append(infoLogs, 
			fmt.Sprintf(
				"--> Load    | %s| No data",
				helpers.RightPad(p.input.tablename, " ", 20-tlen)))

		// Save registry
		var timetosave time.Time
		
		// if enddate after the current time, we keep the last clock found in the last dataset
		if (startimerfc.Add(time.Hour * time.Duration(p.input.hoursperbatch))).After(time.Now()) {
			timetosave = maxclock
		} else {
			timetosave = startimerfc.Add(time.Hour * time.Duration(p.input.hoursperbatch))
		}
		
		registry.Save(config, 
			p.input.tablename, 
			timetosave.Format(time.RFC3339))
			
		infoLogs = append(infoLogs, 
			fmt.Sprintf(
				"--- Waiting | %s| %v sec ",
				helpers.RightPad(p.input.tablename, " ", 20-len(p.input.tablename)),
				p.input.interval))
				
    	print(infoLogs)
	
		return nil
	}

	// --> Load
	startwatch = time.Now()
	inlineData := ""

	if rowcount <= p.output.outputrowsperbatch {

		inlineData = strings.Join(ext.Result[:], "\n")

		loa := influx.NewLoader(
			fmt.Sprintf(
				"%s/write?db=%s&precision=%s",
				p.output.address,
				p.output.database,
				p.output.precision),
			    p.output.username,
			    p.output.password,
			    inlineData)

		err := loa.Load()
		
		if err != nil {
			log.Error(1, "Error while loading data: %s", err)
			return err
		}

		infoLogs = append(infoLogs, 
			fmt.Sprintf(
				"--> Load    | %s| %v rows | took %s",
				helpers.RightPad(p.input.tablename, " ", 20-tlen),
				rowcount,
				time.Since(startwatch)))

	} else { // split result in multiple batches

		var batches float64 = float64(rowcount) / float64(p.output.outputrowsperbatch)
		var batchesCeiled float64 = math.Ceil(batches)
		var batchLoops int = 1
		var minRange int = 0
		var maxRange int = 0

		for batches > 0 { // while
			if batchLoops == 1 {
				minRange = 0
			} else {
				minRange = maxRange
			}

			maxRange = batchLoops * p.output.outputrowsperbatch
			if maxRange >= rowcount {
				maxRange = rowcount 
			}

			// create slide
			datapart := []string{}
			for i := minRange; i < maxRange; i++ {
				datapart = append(datapart, ext.Result[i])
			}

			inlineData = strings.Join(datapart[:], "\n")

			startwatch = time.Now()
			loa := influx.NewLoader(
				fmt.Sprintf(
					"%s/write?db=%s&precision=%s",
					p.output.address,
					p.output.database,
					p.output.precision),
				p.output.username,
				p.output.password,
				inlineData)

			err := loa.Load()
			if err != nil {
				log.Error(1, "Error while loading data: %s", err)
				return err
			}

			prettyTableName := fmt.Sprintf("%s (%v/%v)",
				p.input.tablename,
				batchLoops,
				batchesCeiled)

			tlen = len(prettyTableName)

			infoLogs = append(infoLogs, 
				fmt.Sprintf("--> Load    | %s| %v rows | took %s",
				helpers.RightPad(prettyTableName, " ", 20-tlen),
				len(datapart),
				time.Since(startwatch)))

			batchLoops += 1
			batches -= 1
		}
	}

	// Save registry
	var timetosave time.Time
	if (startimerfc.AddDate(0, 0, p.input.hoursperbatch)).After(time.Now()) {
		timetosave = maxclock
	} else {
		timetosave = startimerfc.Add(time.Hour * time.Duration(p.input.hoursperbatch))
	}
	
	registry.Save(config, 
		p.input.tablename, 
		timetosave.Format(time.RFC3339))

	tlen = len(p.input.tablename)
	infoLogs = append(infoLogs, 
		fmt.Sprintf("--- Waiting | %s| %v sec ",
		helpers.RightPad(p.input.tablename, " ", 20-tlen),
		p.input.interval))

    print(infoLogs)
	
	return nil
}

func print(infoLogs []string) {
	for i := 0; i < len(infoLogs); i++ {
		log.Info(infoLogs[i])
	}
}


//
// Gather data loop
//
func (p *Param) gather() error {
	for {
		err := p.gatherData()
		if err != nil {
			return err
		}

		time.Sleep(time.Duration(p.input.interval) * time.Second)
	}
	return nil
}

//
// Init
//
func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

//
//  Read TOML configuration
//
func readConfig() {

	// command-line flag parsing
	flag.Parse()

	// read configuration file
	if err := cfg.Parse(&config); err != nil {
		fmt.Println(err)
		return
	}
	// validate configuration file
	if err := cfg.Validate(&config); err != nil {
		fmt.Println(err)
		return
	}
}

//
// Read the registry file
//
func readRegistry() {
	if err := registry.Read(&config, &mapTables); err != nil {
		log.Error(0, err.Error())
		return
	}
}

//
// Init global logging
//
func initLog() {
	log.Init(config)
}

//
// Listen to System Signals
//
func listenToSystemSignals() {
	signalChan := make(chan os.Signal, 1)
	code := 0

	signal.Notify(signalChan, os.Interrupt)
	signal.Notify(signalChan, os.Kill)
	signal.Notify(signalChan, syscall.SIGTERM)

	select {
	case sig := <-signalChan:
		log.Info("Received signal %s. shutting down", sig)
	case code = <-exitChan:
		switch code {
		case 0:
			log.Info("Shutting down")
		default:
			log.Warn("Shutting down")
		}
	}
	log.Close()
	os.Exit(code)
}

//
// Main
//
func main() {

	log.Info("***** Starting influxdb-zabbix *****")

	// listen to System Signals
	go listenToSystemSignals()

	readConfig()
	readRegistry()
	initLog()

	// set of active tables
	log.Trace("--- Active tables:")
	var tables = []*cfg.Table{}
	for _, table := range config.Tables {
		if table.Active {
			var tlen int = len(table.Name)

			log.Trace(
				fmt.Sprintf(
					"----------- | %s | Each %v sec | Hours per batch %v | Output %v",
					helpers.RightPad(table.Name, " ", 20-tlen),
					table.Interval,
					table.Hoursperbatch,
					table.Outputrowsperbatch))

			tables = append(tables, table)
		}
	}

	log.Info("--- Start polling")

	var provider string = (reflect.ValueOf(config.Zabbix).MapKeys())[0].String()
	var address string = config.Zabbix[provider].Address
	log.Trace(fmt.Sprintf("--- Provider: %s", provider))

	influxdb := config.InfluxDB

	for _, table := range tables {

		input := Input{
			provider,
			address,
			table.Name,
			table.Interval,
			table.Hoursperbatch}

		output := Output{
			influxdb.Url,
			influxdb.Database,
			influxdb.Username,
			influxdb.Password,
			influxdb.Precision,
			table.Outputrowsperbatch}

		p := &Param{input, output}

		wg.Add(1)
		go p.gather()
	}
	wg.Wait()
}