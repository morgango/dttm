REGISTER '/mnt/hgfs/vmware/projects/pig/dttm.py' USING jython AS dttm;

-- Remove existing files
rmf this_yr;
rmf this_mo;
rmf this_wk;
rmf this_iwk;
rmf this_qtr;
rmf this_wkd;
rmf this_doy;
rmf this_dow;
rmf this_day;
rmf this_hr;
rmf this_min;
rmf this_sec;
rmf this_msec;
rmf this_epoch;

-- Load the test data
dts = LOAD '/user/cloudera/dates' AS (timestamp:chararray);

-- Filter the test data
this_yr = FILTER dts BY dttm.year(timestamp) == 2013 ;
this_qtr = FILTER this_yr BY dttm.quarter(timestamp) == 1 ;
this_mo = FILTER this_yr BY dttm.month(timestamp) == 2 ;
this_iwk = FILTER this_mo BY dttm.iso_week(timestamp) == 6 ;
this_wk = FILTER this_mo BY dttm.week(timestamp) == 6 ;
this_wkd = FILTER this_mo BY dttm.dayname(timestamp) == 'Sunday' ;
this_doy = FILTER this_mo BY dttm.dayofyear(timestamp) == 40 ;
this_dow = FILTER this_mo BY dttm.dayofweek(timestamp) == 1 ;
this_day = FILTER this_mo BY dttm.day(timestamp) == 20 ;
this_hr = FILTER this_day BY dttm.hour(timestamp) == 18 ;
this_min = FILTER this_hr BY dttm.minute(timestamp) == 15 ;
this_sec = FILTER this_min BY dttm.second(timestamp) == 44 ;
this_msec = FILTER this_min BY dttm.microsecond(timestamp) == 0 ;
this_epoch = FILTER this_min BY dttm.epoch(timestamp) > 0 ;

-- add unix date time 

-- Store the results
STORE this_yr INTO 'this_yr' USING PigStorage('*') ; 
STORE this_qtr INTO 'this_qtr' USING PigStorage('*') ; 
STORE this_mo INTO 'this_mo' USING PigStorage('*') ; 
STORE this_iwk INTO 'this_iwk' USING PigStorage('*') ; 
STORE this_wk INTO 'this_wk' USING PigStorage('*') ; 
STORE this_wkd INTO 'this_wkd' USING PigStorage('*') ; 
STORE this_doy INTO 'this_doy' USING PigStorage('*') ; 
STORE this_dow INTO 'this_dow' USING PigStorage('*') ; 
STORE this_day INTO 'this_day' USING PigStorage('*') ; 
STORE this_hr INTO 'this_hr' USING PigStorage('*') ; 
STORE this_min INTO 'this_min' USING PigStorage('*') ; 
STORE this_sec INTO 'this_sec' USING PigStorage('*') ; 
STORE this_msec INTO 'this_msec' USING PigStorage('*') ; 
STORE this_epoch INTO 'this_epoch' USING PigStorage('*') ; 

