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
	provider      string
	address       string
	tablename     string
	interval      int
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
	var currTable string = p.input.tablename
	var currTableForLog string = helpers.RightPad(currTable, " ", 12-len(currTable))

	//start watcher
	startwatch := time.Now()

	// read registry
	if err := registry.Read(&config, &mapTables); err != nil {
		fmt.Println(err)
		return err
	}

	// set times
	starttimereg := registry.GetValueFromKey(mapTables, currTable)
	startimerfc, err := time.Parse("2006-01-02T15:04:05", starttimereg)
	if err != nil {
		startimerfc, err = time.Parse(time.RFC3339, starttimereg)
		if err != nil {
			return err
		}
	}
	var starttimestr string = strconv.FormatInt(startimerfc.Unix(), 10)
	var endtimetmp time.Time = startimerfc.Add(time.Hour * time.Duration(p.input.hoursperbatch))
	var endtimestr string = strconv.FormatInt(endtimetmp.Unix(), 10)

	//
	// <--  Extract
	//
	var tlen int = len(currTable)
	infoLogs = append(infoLogs,
		fmt.Sprintf(
			"----------- | %s | [%v --> %v[",
			currTableForLog,
			startimerfc.Format("2006-01-02 15:04:00"),
			endtimetmp.Format("2006-01-02 15:04:00")))

	ext := input.NewExtracter(
		p.input.provider,
		p.input.address,
		currTable,
		starttimestr,
		endtimestr)

	if err := ext.Extract(); err != nil {
		log.Error(1, "Error while executing script: %s", err)
		return err
	}

	// count rows
	var rowcount int = len(ext.Result)
	infoLogs = append(infoLogs,
		fmt.Sprintf(
			"<-- Extract | %s | %v rows in %s",
			currTableForLog,
			rowcount,
			time.Since(startwatch)))
			
    // set max clock time
	var maxclock time.Time = startimerfc
	if ext.Maxclock.IsZero() == false {
		maxclock = ext.Maxclock
	}

	// no row
	if rowcount == 0 {
		infoLogs = append(infoLogs,
			fmt.Sprintf(
				"--> Load    | %s | No data",
				currTableForLog))
	} else {
		//
		// --> Load
		//
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

			if err := loa.Load(); err != nil {
				log.Error(1, "Error while loading data for %s. %s", currTable, err)
				return err
			}

			infoLogs = append(infoLogs,
				fmt.Sprintf(
					"--> Load    | %s | %v rows in %s",
					currTableForLog,
					rowcount,
					time.Since(startwatch)))

		} else { // else split result in multiple batches

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

				if err := loa.Load(); err != nil {
					log.Error(1, "Error while loading data for %s. %s", currTable, err)
					return err
				}

				// log
				tableBatchName := fmt.Sprintf("%s (%v/%v)",
					currTable,
					batchLoops,
					batchesCeiled)

				tlen = len(tableBatchName)

				infoLogs = append(infoLogs,
					fmt.Sprintf("--> Load    | %s | %v rows in %s",
						helpers.RightPad(tableBatchName, " ", 13-tlen),
						len(datapart),
						time.Since(startwatch)))

				batchLoops += 1
				batches -= 1

			} // end while
		}
	}

	// Save in registry
	saveMaxTime(currTable, startimerfc, maxclock, p.input.hoursperbatch)

	tlen = len(currTable)
	infoLogs = append(infoLogs,
		fmt.Sprintf("--- Waiting | %s | %v sec ",
			currTableForLog,
			p.input.interval))

	// print all log messages
	print(infoLogs)

	return nil
}

//
// Print all messages
//
func print(infoLogs []string) {
	for i := 0; i < len(infoLogs); i++ {
		log.Info(infoLogs[i])
	}
}

//
// Save max time 
//
func saveMaxTime(tablename string, starttime time.Time, maxtime time.Time, duration int) {

	var timetosave time.Time

	// if maxtime is greater than now, keep the maxclock returned 
	if (starttime.Add(time.Hour * time.Duration(duration))).After(time.Now()) {
		timetosave = maxtime
	} else {
		timetosave = starttime.Add(time.Hour * time.Duration(duration))
	}

	registry.Save(config,
		tablename,
		timetosave.Format(time.RFC3339))
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
// Read registry file
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
			var durationh string
			var duration time.Duration = time.Duration(table.Hoursperbatch) * time.Hour
			if (duration.Hours() >= 24) {
				durationh = fmt.Sprintf("%v days per batch", duration.Hours()/24)
			} else {
				durationh = fmt.Sprintf("%v hours per batch", duration.Hours())
			}


			log.Trace(
				fmt.Sprintf(
					"----------- | %s | Each %v sec | %s | Output by %v",
					helpers.RightPad(table.Name, " ", 12-tlen),
					table.Interval,
					durationh,
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
