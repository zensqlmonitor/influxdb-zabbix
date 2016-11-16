package postgresql

import (
	"database/sql"
	"strings"
	"strconv"
	"time"
  "github.com/zensqlmonitor/influxdb-zabbix/input"
  
	_"github.com/lib/pq"
)

type Input input.Input



func NewExtracter(address string, tablename string, startdate string) Input {
    i := Input{}
    i.Address = address
    i.Tablename = tablename
    i.Startdate = startdate
    return i
}

func (i *Input) Extract() error {

	if i.Address == "" || i.Address == "localhost" {
		i.Address = localhost
	}
	
	conn, err := sql.Open("postgres", i.Address)
	if err != nil {
		return err
	}
	defer conn.Close()


	startdateEpoch := time.Now() 
  
	if len(i.Startdate) > 0 {
		startdateEpoch, err = time.Parse("2006-01-02T15:04:05", i.Startdate)
		if err != nil {
			startdateEpoch, err = time.Parse(time.RFC3339, i.Startdate)
      		if err != nil {
           return err
      }
		}
	} 

	// EndDate it the time just before executing query
	t := time.Now()
	i.Enddate = t.Format(time.RFC3339)	
	
	query := strings.Replace(
             strings.Replace(
                queries[i.Tablename], 
                "##STARTDATE##", strconv.FormatInt(startdateEpoch.Unix(), 10), -1),
                "##ENDDATE##", strconv.FormatInt(t.Unix(), 10), -1)
	// log.Trace(
	// 		fmt.Sprintf(
	// 			"------ Query used for %s: %s",
  //       i.tablename,
	// 			query))
        
	rows, err := conn.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// fetch result
    resultTmp := []string{}
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return err
		}
		resultTmp = append(resultTmp, result)
	}
	i.Result = resultTmp

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

var localhost = "host=localhost sslmode=disable"

type MapQuery map[string]string

var queries MapQuery

func init() {
	queries = make(MapQuery)
	queries["compareSize"] = sqlCompare
	queries["history"] = sqlHistory
	queries["history_uint"] = sqlHistoryUInt
	queries["history_log"] = sqlHistoryLog
	queries["trends"] = sqlTrends
	queries["trends_uint"] = sqlTrendsUInt
}

const sqlOne string = `SELECT 1;`

const sqlCompare string = `SELECT result FROM public.trends_influxdb`

const sqlTrends string = `SELECT 
-- measurement
replace(replace(CASE
    WHEN (position('$2' in ite.name) > 0) AND (position('$4' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2)), '$4', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 4))
    WHEN (position('$1' in ite.name) > 0) AND (position('$2' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1)), '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2))
    WHEN (position('$1' in ite.name) > 0) 
       THEN replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1))
    WHEN (position('$2' in ite.name) > 0) 
       THEN replace(ite.name, '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2))
    ELSE ite.name
  END, ',', ''), ' ', '\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\ ')
|| ',group_name=' || replace(grp.name, ' ', '\ ')
|| ',applications=' || replace((SELECT string_agg(app.name, ' | ')
    FROM public.items_applications iap
    INNER JOIN public.applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\ ')
|| ' value_min=' || CAST(tre.value_min as varchar(32))
|| ',value_avg=' || CAST(tre.value_avg as varchar(32))
|| ',value_max=' || CAST(tre.value_max as varchar(32))
-- timestamp (in ms)
|| ' ' || CAST((tre.clock * 1000.) as char(14)) as result
FROM public.trends tre
INNER JOIN public.items ite on ite.itemid = tre.itemid
INNER JOIN public.hosts hos on hos.hostid = ite.hostid
INNER JOIN public.hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN public.groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND tre.clock > ##STARTDATE##
   AND tre.clock <= ##ENDDATE##;
`
const sqlTrendsUInt string = `SELECT 
-- measurement
replace(replace(CASE
    WHEN (position('$2' in ite.name) > 0) AND (position('$4' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2)), '$4', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 4))
    WHEN (position('$1' in ite.name) > 0) AND (position('$2' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1)), '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2))
    WHEN (position('$1' in ite.name) > 0) 
       THEN replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1))
    WHEN (position('$2' in ite.name) > 0) 
       THEN replace(ite.name, '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2))
    ELSE ite.name
  END, ',', ''), ' ', '\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\ ')
|| ',group_name=' || replace(grp.name, ' ', '\ ')
|| ',applications=' || replace((SELECT string_agg(app.name, ' | ')
    FROM public.items_applications iap
    INNER JOIN public.applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\ ')
|| ' value_min=' || CAST(tre.value_min as varchar(32))
|| ',value_avg=' || CAST(tre.value_avg as varchar(32))
|| ',value_max=' || CAST(tre.value_max as varchar(32))
-- timestamp (in ms)
|| ' ' || CAST((tre.clock * 1000.) as char(14)) as result
FROM public.trends_uint tre
INNER JOIN public.items ite on ite.itemid = tre.itemid
INNER JOIN public.hosts hos on hos.hostid = ite.hostid
INNER JOIN public.hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN public.groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND tre.clock > ##STARTDATE##
   AND tre.clock <= ##ENDDATE##;
`

const sqlHistory string = `SELECT 
-- measurement
replace(replace(CASE
    WHEN (position('$2' in ite.name) > 0) AND (position('$4' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2)), '$4', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 4))
    WHEN (position('$1' in ite.name) > 0) AND (position('$2' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1)), '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2))
    WHEN (position('$1' in ite.name) > 0) 
       THEN replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1))
    WHEN (position('$2' in ite.name) > 0) 
       THEN replace(ite.name, '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2))
    ELSE ite.name
  END, ',', ''), ' ', '\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\ ')
|| ',group_name=' || replace(grp.name, ' ', '\ ')
|| ',applications=' || replace((SELECT string_agg(app.name, ' | ')
    FROM public.items_applications iap
    INNER JOIN public.applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\ ')
|| ' value=' || CAST(his.value as varchar(32))
-- timestamp (in ms)
|| ' ' || CAST((his.clock * 1000.) + round(his.ns / 1000000., 0) as char(14)) as result
FROM public.history his
INNER JOIN public.items ite on ite.itemid = his.itemid
INNER JOIN public.hosts hos on hos.hostid = ite.hostid
INNER JOIN public.hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN public.groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND his.clock > ##STARTDATE##
   AND his.clock <= ##ENDDATE##;
`

const sqlHistoryUInt string = `SELECT 
-- measurement
replace(replace(CASE
    WHEN (position('$2' in ite.name) > 0) AND (position('$4' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2)), '$4', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 4))
    WHEN (position('$1' in ite.name) > 0) AND (position('$2' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1)), '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2))
    WHEN (position('$1' in ite.name) > 0) 
       THEN replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1))
	WHEN (position('$2' in ite.name) > 0) 
       THEN replace(ite.name, '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2))
    ELSE ite.name
  END, ',', ''), ' ', '\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\ ')
|| ',group_name=' || replace(grp.name, ' ', '\ ')
|| ',applications=' || replace((SELECT string_agg(app.name, ' | ')
    FROM public.items_applications iap
    INNER JOIN public.applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\ ')
|| ' value=' || CAST(his.value as varchar(32))
-- timestamp (in ms)
|| ' ' || CAST((his.clock * 1000.) + round(his.ns / 1000000., 0) as char(14)) as result
FROM public.history_uint his
INNER JOIN public.items ite on ite.itemid = his.itemid
INNER JOIN public.hosts hos on hos.hostid = ite.hostid
INNER JOIN public.hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN public.groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND his.clock > ##STARTDATE##
   AND his.clock <= ##ENDDATE##;
`

const sqlHistoryLog string = `SELECT 
-- measurement
replace(replace(CASE
    WHEN (position('$2' in ite.name) > 0) AND (position('$4' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2)), '$4', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 4))
    WHEN (position('$1' in ite.name) > 0) AND (position('$2' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1)), '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2))
    WHEN (position('$1' in ite.name) > 0) 
       THEN replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1))
    WHEN (position('$2' in ite.name) > 0) 
       THEN replace(ite.name, '$2', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 2))
    ELSE ite.name
  END, ',', ''), ' ', '\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\ ')
|| ',group_name=' || replace(grp.name, ' ', '\ ')
|| ',applications=' || replace((SELECT string_agg(app.name, ' | ')
    FROM public.items_applications iap
    INNER JOIN public.applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\ ')
|| ' value=' || CAST(his.value as varchar(32))
-- timestamp (in ms)
|| ' ' || CAST((his.clock * 1000.) + round(his.ns / 1000000., 0) as char(14)) as result
FROM public.history_log his
INNER JOIN public.items ite on ite.itemid = his.itemid
INNER JOIN public.hosts hos on hos.hostid = ite.hostid
INNER JOIN public.hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN public.groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND his.clock > ##STARTDATE##
   AND his.clock <= ##ENDDATE##;
`