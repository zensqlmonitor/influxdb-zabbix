package input

import (
	"database/sql"
	"strings"
	"strconv"
	"time"
	
	_"github.com/lib/pq"
	_"github.com/go-sql-driver/mysql"
)


type Input struct {
    Provider   string
    Address    string
	Tablename  string
	Rowsperbatch int
	Startdate  time.Time
	Enddate    time.Time
	Result     []string
}

func NewExtracter(provider string, address string, tablename string, intputrowsperbatch int, startdate time.Time, enddate time.Time) Input {
    i := Input{}
    i.Provider = provider
    i.Address = address
    i.Tablename = tablename
    i.Rowsperbatch = intputrowsperbatch
    i.Startdate = startdate
    i.Enddate = enddate
    return i
}



func (i *Input) Extract() error {
	
	// set query
	var abstractQuery string
	if (i.Provider == "postgres") {
		abstractQuery = pgqueries[i.Tablename];
	} else {
		abstractQuery = myqueries[i.Tablename];
	}
	query := strings.Replace(
				strings.Replace(
					strings.Replace(
						abstractQuery, 
						"##STARTDATE##", strconv.FormatInt(i.Startdate.Unix(), 10), -1),
						"##ENDDATE##", strconv.FormatInt(i.Enddate.Unix(), 10), -1),
						"##ROWSPERBATCH##", strconv.Itoa(i.Rowsperbatch), -1)
						
	// open a connection
	conn, err := sql.Open(i.Provider, i.Address)
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
  
  // move last clock to the starting date for the next run
    if len(clock) > 0 {
		lastclock, err := msToTime(strings.Trim(clock, " "))
		if err != nil {
		  return err
		}
    i.Startdate = lastclock
  }  

   return nil
}


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
/// SQL code
///
type MapQuery map[string]string

var pgqueries, myqueries MapQuery

func init() {
	pgqueries = make(MapQuery)
	pgqueries["history"] = pgsqlHistory
	pgqueries["history_uint"] = pgsqlHistoryUInt
	pgqueries["trends"] = pgsqlTrends
	pgqueries["trends_uint"] = pgsqlTrendsUInt
	
	myqueries = make(MapQuery)
	myqueries["history"] = mysqlHistory
	myqueries["history_uint"] = mysqlHistoryUInt
	myqueries["trends"] = mysqlTrends
	myqueries["trends_uint"] = mysqlTrendsUInt
}


const pgsqlTrends string = `SELECT 
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
const pgsqlTrendsUInt string = `SELECT 
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

const pgsqlHistory string = `SELECT 
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

const pgsqlHistoryUInt string = `SELECT 
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

const mysqlTrends string = `SELECT 
-- measurement
replace(replace(CASE
    WHEN (position('$2' in ite.name) > 0) AND (position('$4' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1)), '$4', SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-1))
	WHEN (position('$1' in ite.name) > 0) AND (position('$2' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1)), '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1))
    WHEN (position('$1' in ite.name) > 0) 
       THEN replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1), ',',-1))
    WHEN (position('$2' in ite.name) > 0) 
       THEN replace(ite.name, '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1))
	WHEN (position('$3' in ite.name) > 0)
       THEN replace(ite.name, '$3', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-2), ',',1))
    WHEN (position('$1' in ite.name) > 0) AND (position('$3' in ite.name) > 0)
       THEN replace(replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1), ',',-1)), '$3', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-2), ',',1))
    ELSE ite.name
  END, ',', ''), ' ', '\\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\\ ')
|| ',group_name=' || replace(grp.name, ' ', '\\ ')
|| ',applications=' || ifnull(replace(replace((SELECT GROUP_CONCAT(app.name, ' ')
    FROM items_applications iap
    INNER JOIN applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\\ '), ',', ''), 'No application')
|| ' value_min=' || CAST(tre.value_min as char)
|| ',value_avg=' || CAST(tre.value_avg as char)
|| ',value_max=' || CAST(tre.value_max as char)
-- timestamp (in ms)
|| ' ' || CAST((tre.clock * 1000.) as char) as result
,  CAST((tre.clock * 1000.) as char) as clock
FROM trends tre 
INNER JOIN items ite on ite.itemid = tre.itemid
INNER JOIN hosts hos on hos.hostid = ite.hostid
INNER JOIN hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND tre.clock > ##STARTDATE##
   AND tre.clock <= ##ENDDATE##
ORDER BY tre.clock ASC, tre.itemid ASC
LIMIT ##ROWSPERBATCH##;
`


const mysqlTrendsUInt string = `SELECT 
-- measurement
replace(replace(CASE
    WHEN (position('$2' in ite.name) > 0) AND (position('$4' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1)), '$4', SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-1))
	WHEN (position('$1' in ite.name) > 0) AND (position('$2' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1)), '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1))
    WHEN (position('$1' in ite.name) > 0) 
       THEN replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1), ',',-1))
    WHEN (position('$2' in ite.name) > 0) 
       THEN replace(ite.name, '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1))
	WHEN (position('$3' in ite.name) > 0)
       THEN replace(ite.name, '$3', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-2), ',',1))
    WHEN (position('$1' in ite.name) > 0) AND (position('$3' in ite.name) > 0)
       THEN replace(replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1), ',',-1)), '$3', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-2), ',',1))
    ELSE ite.name
  END, ',', ''), ' ', '\\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\\ ')
|| ',group_name=' || replace(grp.name, ' ', '\\ ')
|| ',applications=' || ifnull(replace(replace((SELECT GROUP_CONCAT(app.name, ' ')
    FROM items_applications iap
    INNER JOIN applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\\ '), ',', ''), 'No application')
|| ' value_min=' || CAST(tre.value_min as char)
|| ',value_avg=' || CAST(tre.value_avg as char)
|| ',value_max=' || CAST(tre.value_max as char)
-- timestamp (in ms)
|| ' ' || CAST((tre.clock * 1000.) as char) as result
,  CAST((tre.clock * 1000.) as char) as clock
FROM trends tre 
INNER JOIN items ite on ite.itemid = tre.itemid
INNER JOIN hosts hos on hos.hostid = ite.hostid
INNER JOIN hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND tre.clock > ##STARTDATE##
   AND tre.clock <= ##ENDDATE##
ORDER BY tre.clock ASC, tre.itemid ASC
LIMIT ##ROWSPERBATCH##;
`

const mysqlHistory string = `SELECT 
-- measurement
replace(replace(CASE
    WHEN (position('$2' in ite.name) > 0) AND (position('$4' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1)), '$4', SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-1))
	WHEN (position('$1' in ite.name) > 0) AND (position('$2' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1)), '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1))
    WHEN (position('$1' in ite.name) > 0) 
       THEN replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1), ',',-1))
    WHEN (position('$2' in ite.name) > 0) 
       THEN replace(ite.name, '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1))
	WHEN (position('$3' in ite.name) > 0)
       THEN replace(ite.name, '$3', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-2), ',',1))
    WHEN (position('$1' in ite.name) > 0) AND (position('$3' in ite.name) > 0)
       THEN replace(replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1), ',',-1)), '$3', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-2), ',',1))
    ELSE ite.name
  END, ',', ''), ' ', '\\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\\ ')
|| ',group_name=' || replace(grp.name, ' ', '\\ ')
|| ',applications=' || ifnull(replace(replace((SELECT GROUP_CONCAT(app.name, ' ')
    FROM items_applications iap
    INNER JOIN applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\\ '), ',', ''), 'No application')
|| ' value=' || CAST(his.value as char)
-- timestamp (in ms)
|| ' ' || CAST((his.clock * 1000.) as char) as result
,  CAST((his.clock * 1000.) + round(his.ns / 1000000., 0) as char) as clock
FROM history his
INNER JOIN items ite on ite.itemid = his.itemid
INNER JOIN hosts hos on hos.hostid = ite.hostid
INNER JOIN hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND his.clock > ##STARTDATE##
   AND his.clock <= ##ENDDATE##
ORDER BY his.clock ASC, his.ns ASC, his.itemid ASC
LIMIT ##ROWSPERBATCH##;
`

const mysqlHistoryUInt string = `SELECT 
-- measurement
replace(replace(CASE
    WHEN (position('$2' in ite.name) > 0) AND (position('$4' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1)), '$4', SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-1))
	WHEN (position('$1' in ite.name) > 0) AND (position('$2' in ite.name) > 0) 
      THEN replace(replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1)), '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1))
    WHEN (position('$1' in ite.name) > 0) 
       THEN replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1), ',',-1))
    WHEN (position('$2' in ite.name) > 0) 
       THEN replace(ite.name, '$2', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',2), ',',-1))
	WHEN (position('$3' in ite.name) > 0)
       THEN replace(ite.name, '$3', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-2), ',',1))
    WHEN (position('$1' in ite.name) > 0) AND (position('$3' in ite.name) > 0)
       THEN replace(replace(ite.name, '$1', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',1), ',',-1)), '$3', SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(ite.key_, LOCATE('[',ite.key_) + 1, LOCATE(']',ite.key_) - LOCATE('[',ite.key_)-1),',',-2), ',',1))
    ELSE ite.name
  END, ',', ''), ' ', '\\ ') 
-- tags
|| ',host_name=' || replace(hos.name, ' ', '\\ ')
|| ',group_name=' || replace(grp.name, ' ', '\\ ')
|| ',applications=' || ifnull(replace(replace((SELECT GROUP_CONCAT(app.name, ' ')
    FROM items_applications iap
    INNER JOIN applications app on app.applicationid = iap.applicationid
    WHERE iap.itemid = ite.itemid), ' ', '\\ '), ',', ''), 'No application')
|| ' value=' || CAST(his.value as char)
-- timestamp (in ms)
|| ' ' || CAST((his.clock * 1000.) as char) as result
,  CAST((his.clock * 1000.) + round(his.ns / 1000000., 0) as char) as clock
FROM history_uint his
INNER JOIN items ite on ite.itemid = his.itemid
INNER JOIN hosts hos on hos.hostid = ite.hostid
INNER JOIN hosts_groups hg on hg.hostid = hos.hostid
INNER JOIN groups grp on grp.groupid = hg.groupid
WHERE grp.internal=0
   AND his.clock > ##STARTDATE##
   AND his.clock <= ##ENDDATE##
ORDER BY his.clock ASC, his.ns ASC, his.itemid ASC
LIMIT ##ROWSPERBATCH##;
`