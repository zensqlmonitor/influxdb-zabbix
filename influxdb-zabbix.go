package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"reflect"
	"runtime"
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
	provider          string
	address           string
	tablename         string
	interval          int
	inputrowsperbatch int
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

	// read registry
	if err := registry.Read(&config, &mapTables); err != nil {
		fmt.Println(err)
		return err
	}

	start := time.Now()
	enddate := start
	startdateEpoch := start
	startdate := registry.GetValueFromKey(mapTables, p.input.tablename)

	// no start date configured ? return
	if len(startdate) == 0 {
		log.Fatal(1, "No startdate defined for table %s", p.input.tablename)
		return nil
	}

	// format start date
	startdateEpoch, err := time.Parse("2006-01-02T15:04:05", startdate)
	if err != nil {
		startdateEpoch, err = time.Parse(time.RFC3339, startdate)
		if err != nil {
			return err
		}
	}

	var tlen int = len(p.input.tablename)
	var loopnr int = 0
	var ext input.Input

	// <--  Extract loop
	for {
		if ext.Tablename == "" {
			ext = input.NewExtracter(
				p.input.provider,
				p.input.address,
				p.input.tablename,
				p.input.inputrowsperbatch,
				startdateEpoch,
				enddate)
		}

		loopnr += 1
		log.Trace(
			fmt.Sprintf(
				"--- Table %s | Loop #%v | Starting from %s | Limit %v",
				ext.Tablename,
				loopnr,
				ext.Startdate,
				ext.Rowsperbatch))

		if err := ext.Extract(); err != nil {
			log.Error(1, "Error while executing script: %s", err)
			return err
		}

		var rowcount int = len(ext.Result)
		rows := make([]string, rowcount)
		copy(rows, ext.Result)

		log.Info(
			fmt.Sprintf(
				"<-- Extract | %s| %v rows | took %s",
				helpers.RightPad(p.input.tablename, " ", 20-tlen),
				rowcount,
				time.Since(start)))

		/// no row, no load
		if rowcount == 0 {
			log.Info(
				fmt.Sprintf(
					"--> Load    | %s| No data",
					helpers.RightPad(p.input.tablename, " ", 20-tlen)))

			// Save registry & break
			registry.Save(config, p.input.tablename, ext.Enddate.Format(time.RFC3339))

			log.Info(
				fmt.Sprintf(
					"--- Waiting | %s| %v sec ",
					helpers.RightPad(p.input.tablename, " ", 20-len(p.input.tablename)),
					p.input.interval))

			break
		}

		// --> Load
		start = time.Now()
		inlineData := ""

		if rowcount <= p.output.outputrowsperbatch {
			inlineData = strings.Join(rows[:], "\n")
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

			log.Info(
				fmt.Sprintf(
					"--> Load    | %s| %v rows | took %s",
					helpers.RightPad(p.input.tablename, " ", 20-tlen),
					rowcount,
					time.Since(start)))

		} else { // multiple batches

			var batches float64 = float64(rowcount) / float64(p.output.outputrowsperbatch)
			var batchesCeiled float64 = math.Ceil(batches)
			var batchLoops int = 1
			var minRange int = 0
			var maxRange int = 0

			for batches > 0 { // while
				if batchLoops == 1 {
					minRange = 0
				} else {
					minRange = maxRange + 1
				}

				maxRange = batchLoops * p.output.outputrowsperbatch
				if maxRange >= rowcount {
					maxRange = rowcount - 1
				}

				// create slide
				datapart := []string{}
				for i := minRange; i <= maxRange; i++ {
					datapart = append(datapart, rows[i])
				}

				inlineData = strings.Join(datapart[:], "\n")

				start = time.Now()
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

				log.Info(fmt.Sprintf("--> Load    | %s| %v rows | took %s",
					prettyTableName,
					len(datapart),
					time.Since(start)))

				batchLoops += 1
				batches -= 1
			}
		}

		// Save registry
		registry.Save(config, p.input.tablename, ext.Enddate.Format(time.RFC3339))
		
		tlen = len(p.input.tablename)
		log.Info(fmt.Sprintf("--- Waiting | %s| %v sec ",
			helpers.RightPad(p.input.tablename, " ", 20-tlen),
			p.input.interval))

	} // end for

	return nil
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

	// list of active tables
	log.Trace("--- Active tables:")
	var tables = []*cfg.Table{}
	for _, table := range config.Tables {
		if table.Active {
			log.Trace(
				fmt.Sprintf(
					"------ %s with an interval of %v sec | InputRowsPerBatch = %v | OutputRowsPerBatch = %v",
					table.Name,
					table.Interval,
					table.Inputrowsperbatch,
					table.Outputrowsperbatch))

			tables = append(tables, table)
		}
	}

	
	log.Info("--- Start polling")

	var provider string = (reflect.ValueOf(config.Zabbix).MapKeys())[0].String()
	var address string = config.Zabbix[provider].Address;
	log.Trace(fmt.Sprintf("--- Provider:"))
	log.Trace(fmt.Sprintf("------ %s", provider))
	
	influxdb := config.InfluxDB

	for _, t := range tables {
	
		input := Input{
			provider,
			address,
			t.Name,
			t.Interval,
			t.Inputrowsperbatch}

		output := Output{
			influxdb.Url,
			influxdb.Database,
			influxdb.Username,
			influxdb.Password,
			influxdb.Precision,
			t.Outputrowsperbatch}

		p := &Param{input, output}

		wg.Add(1)
		go p.gather()
	}
	wg.Wait()
}
