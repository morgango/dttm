REGISTER '/mnt/hgfs/vmware/projects/pig/dttm.py' USING jython AS dttm;

-- Remove existing files
rmf dts
rmf scratch;
rmf scratch2;
rmf scratch3;


-- Load the test data
dts = LOAD '/user/cloudera/dates' AS (timestamp:chararray);

-- Filter the test data

this_yr = FILTER dts BY dttm.year(timestamp) == 2013 ;
this_qtr = FILTER this_yr BY dttm.quarter(timestamp) == 1 ;

scratch = FOREACH this_qtr 
		  GENERATE 
				timestamp, 
				dttm.date_name('dt',(timestamp)) as dayname,
				dttm.date_name('yy',(timestamp)) as year,
				dttm.date_name('mm',(timestamp)) as month,
				dttm.date_name('dd',(timestamp)) as day,
				dttm.date_name('tz',(timestamp)) as tz
		;

scratch2 = FOREACH this_qtr 
		  GENERATE 
				timestamp, 
				dttm.date_add('second',1,timestamp) as nextsecond,
				dttm.date_add('minute',1,timestamp) as nextminute,
				dttm.date_add('hour',1,timestamp) as nexthour,
				dttm.date_add('day',1,timestamp) as nextday,
				dttm.date_add('week',1,timestamp) as nextweek,
				dttm.date_add('month',1,timestamp) as nextmonth,
				dttm.date_add('quarter',1,timestamp) as nextquarter,
				dttm.date_add('year',1,timestamp) as nextyear
		;
		
scratch3 = FOREACH this_qtr 
		  GENERATE 
				timestamp, 
				dttm.now() as now,
				dttm.today() as today,
				dttm.second(timestamp) as secs,
				dttm.date_add('day',-1, dttm.today()) as yesterday,
				dttm.date_diff('second', dttm.date_add('day',-1, dttm.today()) , dttm.now()) as ddiff,
				dttm.date_end_of('day', timestamp) as eod,
				dttm.date_end_of('month', timestamp) as eom,
				dttm.date_start_of('quarter', timestamp) as qtr,
				dttm.date_end_of('quarter', timestamp) as eoq,
				dttm.temporal_from_parts(2000,01,01) as parts
		;

-- Store the results
STORE scratch INTO 'scratch' USING PigStorage() ; 
STORE scratch2 INTO 'scratch2' USING PigStorage() ; 
STORE scratch3 INTO 'scratch3' USING PigStorage() ; 
STORE dts INTO 'dts' USING PigStorage() ; 

