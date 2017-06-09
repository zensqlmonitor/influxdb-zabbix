package input

import (
	"database/sql"
	"strings"
	"time"
	
	helpers "github.com/zensqlmonitor/influxdb-zabbix/helpers"
	
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type Input struct {
	Provider  string
	Address   string
	Tablename string
	Starttime string
	Endtime   string
	Maxclock  time.Time
	Result    []string
}

func NewExtracter(provider string, address string, tablename string, starttime string, endtime string) Input {
	i := Input{}
	i.Provider = provider
	i.Address = address
	i.Tablename = tablename
	i.Starttime = starttime
	i.Endtime = endtime
	return i
}

func (input *Input) getSQL() string {

	var query string

	switch input.Provider {
	case "postgres":
		query = pgSQL(input.Tablename)
	case "mysql":
		query = mySQL(input.Tablename)
	default:
		panic("unrecognized provider")
	}

	return strings.Replace(
		strings.Replace(
			query,
			"##STARTDATE##", input.Starttime, -1),
		    "##ENDDATE##", input.Endtime, -1)
}

func (input *Input) Extract() error {

	// get query
	query := input.getSQL()

	//fmt.Println(fmt.Sprintf("------------------- %s: %s", input.Tablename, query))

	// open a connection
	conn, err := sql.Open(input.Provider, input.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	rows, err := conn.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// fetch result
	resultInline := []string{}
	var clock string

	for rows.Next() {
		var result string
		if err := rows.Scan(&result, &clock); err != nil {
			return err
		}
		resultInline = append(resultInline, result)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()

	input.Result = resultInline

	// saved max clock from the result set
	if len(clock) > 0 {
		lastclock, err := helpers.MsToTime(strings.Trim(clock, " "))
		if err != nil {
			return err
		}
		input.Maxclock = lastclock
	}

	return nil
}
