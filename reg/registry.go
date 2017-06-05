package registry

import (
	"errors"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"sync"
	
	cfg "github.com/zensqlmonitor/influxdb-zabbix/config"
	log "github.com/zensqlmonitor/influxdb-zabbix/log"
)

type Registry struct {
	Table     string
	Startdate string
}

type MapTable map[string]string

//var mapTables = make(MapTable)

var mu sync.Mutex


func check(e error) {
	if e != nil {
		panic(e)
	}
}

func Read(config *cfg.TOMLConfig, mapTables *MapTable) error {

	if _, err := ioutil.ReadFile(config.Registry.FileName); err != nil {
		Create(config) // create file if not exist
	}

	registryJson, err := ioutil.ReadFile(config.Registry.FileName)
	check(err)

	// parse JSON
	regEntries := make([]Registry, 0)
	if err := json.Unmarshal(registryJson, &regEntries); err != nil {
		return err
	}

	for i := 0; i < len(regEntries); i++ {
		tableName := regEntries[i].Table
		startdate := regEntries[i].Startdate
		SetValueByKey(mapTables, tableName, startdate)
	}

	return nil
}

func Create(config *cfg.TOMLConfig) error {

	if len(config.Tables) == 0 {
		err := errors.New("No tables in configuration")
		check(err)
	}

	regEntries := make([]Registry, len(config.Tables))

	var idx int = 0
	for _, table := range config.Tables {
		var reg Registry
		reg.Table = table.Name
		reg.Startdate = table.Startdate
		regEntries[idx] = reg
		idx += 1
	}

	// write JSON file
	registryOutJson, _ := json.MarshalIndent(regEntries, "", "    ")
	ioutil.WriteFile(config.Registry.FileName, registryOutJson, 0777)
	log.Trace(fmt.Sprintf(
		"------ New registry file created to %s",
		config.Registry.FileName))

	return nil
}

func Save(config cfg.TOMLConfig, tableName string, lastClock string) error {

	// read  file
	registryJson, err := ioutil.ReadFile(config.Registry.FileName)
	check(err)

	// parse JSON
	regEntries := make([]Registry, 0)
	if err := json.Unmarshal(registryJson, &regEntries); err != nil {
		return err
	}
	var found bool = false
	for i := 0; i < len(regEntries); i++ {
		if regEntries[i].Table == tableName {
			regEntries[i].Startdate = lastClock
			found = true
		}
	}
	// if not found, create it
	if found == false {
		regEntries = append(regEntries, Registry{tableName, lastClock})
	}

	// write JSON file
	registryOutJson, _ := json.MarshalIndent(regEntries, "", "    ")
	ioutil.WriteFile(config.Registry.FileName, registryOutJson, 0777)

	return nil
}

func SetValueByKey(mt *MapTable, key string, value string) {
	mu.Lock()
	defer mu.Unlock()
	(*mt)[key] = value
}

func GetValueFromKey(mt MapTable, key string) string {
	if len(mt) > 0 {
		mu.Lock()
		defer mu.Unlock()
		return mt[key]
	}
	return ""
}