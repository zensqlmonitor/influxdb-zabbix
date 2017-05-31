package postgresql

import (
	"database/sql"
	"strings"
	"strconv"
	"time"
        "github.com/zensqlmonitor/influxdb-zabbix/input"
        //"fmt"
  
	_"github.com/lib/pq"
)

///
/// utils
///
const (
	millisPerSecond     = int64(time.Second / time.Millisecond)
	nanosPerMillisecond = int64(time.Millisecond / time.Nanosecond)
)

func msToTime(ms string) (time.Time, error) {
	msInt, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		   return time.Time{}, err
	}
	return time.Unix(msInt/millisPerSecond,
	  (msInt%millisPerSecond)*nanosPerMillisecond), nil
}

///
/// main methods
///
type Input input.Input

func NewExtracter(address string, tablename string, intputrowsperbatch int, startdate time.Time, enddate time.Time) Input {
    i := Input{}
    i.Address = address
    i.Tablename = tablename
    i.Rowsperbatch = intputrowsperbatch
    i.Startdate = startdate
    i.Enddate = enddate
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
	
	query := strings.Replace(
            strings.Replace(
             strings.Replace(
                queries[i.Tablename], 
                "##STARTDATE##", strconv.FormatInt(i.Startdate.Unix(), 10), -1),
                "##ENDDATE##", strconv.FormatInt(i.Enddate.Unix(), 10), -1),
                "##ROWSPERBATCH##", strconv.Itoa(i.Rowsperbatch), -1)
  // fmt.Println(
	//  		fmt.Sprintf(
	//  			"------ Query used for %s: %s",
  //        i.Tablename,
	//  			query))
    
	rows, err := conn.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// fetch result
  resulttmp := []string{}
  var clock string
  
	for rows.Next() {
		var result string
		if err := rows.Scan(&result, &clock); err != nil {
			return err
		}
		resulttmp= append(resulttmp, result)
	}

	if err := rows.Err(); err != nil {
		return err
	}

  rows.Close()

	i.Result = resulttmp
  
  // last clock will be the start date for the next run
  if len(clock) > 0 {
      lastclock, err := msToTime(strings.Trim(clock, " "))
    if err != nil {
      return err
    }
    i.Startdate = lastclock
  }  

	return nil
}

var localhost = "host=localhost sslmode=disable"

type MapQuery map[string]string

var queries MapQuery

func init() {
	queries = make(MapQuery)
	queries["history"] = sqlHistory
	queries["history_uint"] = sqlHistoryUInt
	queries["trends"] = sqlTrends
	queries["trends_uint"] = sqlTrendsUInt
}

const sqlOne string = `SELECT 1;`

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
|| ',applications=' || coalesce(replace((SELECT string_agg(app.name, ' | ')
    FROM public.items_applications iap
    INNER JOIN public.applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\ '), 'No application')
|| ' value_min=' || CAST(tre.value_min as varchar(32))
|| ',value_avg=' || CAST(tre.value_avg as varchar(32))
|| ',value_max=' || CAST(tre.value_max as varchar(32))
-- timestamp (in ms)
|| ' ' || CAST((tre.clock * 1000.) as char(14)) as result
,  CAST((tre.clock * 1000.) as char(14)) as clock
FROM public.trends tre
INNER JOIN public.items ite on ite.itemid = tre.itemid
INNER JOIN public.hosts hos on hos.hostid = ite.hostid
INNER JOIN public.hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN public.groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND tre.clock > ##STARTDATE##
   AND tre.clock <= ##ENDDATE##
ORDER BY tre.clock ASC, tre.itemid ASC
LIMIT ##ROWSPERBATCH##;
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
    WHEN (position('$3' in ite.name) > 0)
       THEN replace(ite.name, '$3', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 3))
    WHEN (position('$1' in ite.name) > 0) AND (position('$3' in ite.name) > 0)
       THEN replace(replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1)), '$3', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 3))
    ELSE ite.name
  END, ',', ''), ' ', '\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\ ')
|| ',group_name=' || replace(grp.name, ' ', '\ ')
|| ',applications=' || coalesce(replace((SELECT string_agg(app.name, ' | ')
    FROM public.items_applications iap
    INNER JOIN public.applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\ '), 'No application')
|| ' value_min=' || CAST(tre.value_min as varchar(32))
|| ',value_avg=' || CAST(tre.value_avg as varchar(32))
|| ',value_max=' || CAST(tre.value_max as varchar(32))
-- timestamp (in ms)
|| ' ' || CAST((tre.clock * 1000.) as char(14)) as result
,  CAST((tre.clock * 1000.) as char(14)) as clock
FROM public.trends_uint tre
INNER JOIN public.items ite on ite.itemid = tre.itemid
INNER JOIN public.hosts hos on hos.hostid = ite.hostid
INNER JOIN public.hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN public.groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND tre.clock > ##STARTDATE##
   AND tre.clock <= ##ENDDATE##
ORDER BY tre.clock ASC, tre.itemid ASC
LIMIT ##ROWSPERBATCH##;
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
    WHEN (position('$3' in ite.name) > 0)
       THEN replace(ite.name, '$3', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 3))
    WHEN (position('$1' in ite.name) > 0) AND (position('$3' in ite.name) > 0)
       THEN replace(replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1)), '$3', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 3))
    ELSE ite.name
  END, ',', ''), ' ', '\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\ ')
|| ',group_name=' || replace(grp.name, ' ', '\ ')
|| ',applications=' || coalesce(replace((SELECT string_agg(app.name, ' | ')
    FROM public.items_applications iap
    INNER JOIN public.applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\ '), 'No application')
|| ' value=' || CAST(his.value as varchar(32))
-- timestamp (in ms)
|| ' ' || CAST((his.clock * 1000.) + round(his.ns / 1000000., 0) as char(14)) as result
,  CAST((his.clock * 1000.) + round(his.ns / 1000000., 0) as char(14)) as clock
FROM public.history his
INNER JOIN public.items ite on ite.itemid = his.itemid
INNER JOIN public.hosts hos on hos.hostid = ite.hostid
INNER JOIN public.hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN public.groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND his.clock > ##STARTDATE##
   AND his.clock <= ##ENDDATE##
ORDER BY his.clock ASC, his.ns ASC, his.itemid ASC
LIMIT ##ROWSPERBATCH##;
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
    WHEN (position('$3' in ite.name) > 0)
       THEN replace(ite.name, '$3', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 3))
    WHEN (position('$1' in ite.name) > 0) AND (position('$3' in ite.name) > 0)
       THEN replace(replace(ite.name, '$1', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 1)), '$3', split_part(substring(ite.key_ FROM '\[(.+)\]'), ',', 3))
    ELSE ite.name
  END, ',', ''), ' ', '\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\ ')
|| ',group_name=' || replace(grp.name, ' ', '\ ')
|| ',applications=' || coalesce(replace((SELECT string_agg(app.name, ' | ')
    FROM public.items_applications iap
    INNER JOIN public.applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\ '), 'No application')
|| ' value=' || CAST(his.value as varchar(32))
-- timestamp (in ms)
|| ' ' || CAST((his.clock * 1000.) + round(his.ns / 1000000., 0) as char(14)) as result
,  CAST((his.clock * 1000.) + round(his.ns / 1000000., 0) as char(14)) as clock
FROM public.history_uint his
INNER JOIN public.items ite on ite.itemid = his.itemid
INNER JOIN public.hosts hos on hos.hostid = ite.hostid
INNER JOIN public.hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN public.groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND his.clock > ##STARTDATE##
   AND his.clock <= ##ENDDATE##
ORDER BY his.clock ASC, his.ns ASC, his.itemid ASC
LIMIT ##ROWSPERBATCH##;
`
