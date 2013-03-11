#!/usr/bin/python

'''
DTTM is a UDF library that aims to make date manipulation simpler and more robust for unstructured data.  
Conceptually, the date/time UDFs in Hadoop are fine, but they seem to be designed to be easy to code, 
and not particularly easy to use for end users.  

The core design principles of the DTTM library are to: 

(1) Allow date and time manipulations from unstructured text, which I refer to as "temporal values".  
DTTM does not requiring specific formatting for dates or times, instead it tries to figure out what is in 
a text string and use it.  Formatting temporals to fit the language is ass-backward, and typically 
this is a time consuming, difficult and error-prone part of data preparation.
	
A temporal value could be anything from "09/25/2003 10:49:41 -03:00" to 
"Today is 25 of September of 2003, exactly at 10:49:41 with timezone -03:00." These should both be usable and
interchangeable without user intervention.
	
(2) Make temporal manipulations very easy to do, so that an analyst could understand them without needing 
much (if any) additional knowledge about data types.
	
(3) Make date and time functions that are friendly to users.  That means having fewer functions with parameters
instead of lots of very similar functions.  Also, include useful utility functions that allow common date manipulations,
such as extracting parts of temporal values (for partitioning) and finding the beginning and end of days, months, and quarters.
	
(4) Make date and time manipulations more durable.  If formatting is slightly different for you shouldn't have
to do a lot of work to compensate.    If your data looks like it could be a datetime to the
eyeball, you should be able to use it for data processing.

(5) Leverage the best APIs from procedural and query based languages and not reinvent the wheel.  Use existing 
code whenever possible and take advantage of platform-specific niceties.  Elements of Python,  
Transact-SQL, and PL/SQL were leveraged  for back- and front-end work.

Usage
---------

Much of DTTM is designed to which is to have a single function with parameters to specify 
how work should be done.  For example, there is one function called date_diff that has a parameter (datepart)
that is used to tell what units should be returned.  This is much easier fo end users to remember and use than
something like one function per unit (date_diff_seconds, date_diff_minutes, date_diff_hours, etc.).
	
In actual operation, the functions that get used the most are: 

	* parse_temporal(), which is used implicitly used by every other function.
	* Manipulation functions (date_add(), date_diff(), date_trunc(), date_start_of(), date_end_of(), date_name()) functions.
	* today() and now() 

There is a common set of string constants that are used across all of the manipulation functions through
the datepart variable:
	
	'year', 'yy', 'yyyy': The numeric calendar year
	'quarter', 'qq', 'q': The numeric calendar quarter (1-4)
	'month', 'mm', 'm': The numeric calendar month (1-12)
	'day_of_year', 'dy', 'y': The day (1-366) of the year.
	'day_name', 'dn': The name ('Sunday') of the day of the week
	'day', 'dd', 'd': The numeric day of the month.
	'week', 'wk', 'ww': The numeric week of the year.
	'weekday', 'dw': The numeric day of the week
	'day_of_week', 'dow': The numeric day of the week (1-7)
	'hour', 'hh': The numeric hour of the day (0-23)
	'minute', 'mi', 'n': The numeric minute of the hour (0-59)
	'second', 'ss', 's': The numeric second of the minute (0-59)
	'microsecond', 'mcs': The numeric microsecond of the second (0-1000000)
	'epoch', 'unix', 'ep': The numeric number of seconds since 1970-01-01 00:00:00
	'TZoffset', 'tz': The time zone offset.
	'ISO_WEEK', 'iso_wk', 'isoww': The numeric week of the year, as defined by ISO standards.
	
All values may not be able to be used in each function, as they may or may not make sense.  For example,
it does not make sense to truncate a temporal value to the microsecond, as this is the lowest increment of
time used with temporal values. 
	
Notes
---------

(1) Internally, DTTM is done in Python, and would first be implemented in Pig using Jython.  I understand that
these could be done using Java, but the weight of the language is simply too much for me to bear for
what should be simple functionality.

(2) When using DTTM functions, you can simply specify your temporal values in a string without a particular
format.  The library is set up so that it will try to convert any text to a proper datetime value, using
the totally awesome Python dateutil library and parser.  A temporal value could be anything from "12/31/1999" to 
"Today is 25 of September of 2003, exactly at 10:49:41 with timezone -03:00." and it would be usable.
	
(3) This library is initially intended for PIG, but may be extended to other Hadoop projects as opportunites arise.

(4) As mentioned before, this library depends on the python datetime and dateutil libraries.  They must be installed
and available in your Jython Path across your cluster.
			
'''
import datetime
from datetime import *

import time

import dateutil
from dateutil import relativedelta, rrule, parser

from dateutil.relativedelta import *
from dateutil.rrule import *
from dateutil.parser import *


def parse_temporal(input_text):
	'''
	Returns a valid python datetime object and returns it as a string. It will attempt to automatically 
	format the input string and will throw an exception if it cannot.
	
	This function is really the heart of the package.  Virtually every other function calls this
	to build a working datetime value.  Working with unstructured data means that we don't necesarily
	have perfectly formatted data, especially for temporal data.  This function helps us work with 
	data that is most likely in string form (or can easily be converted) and get something useful
	out the other end.
	
	NOTE: 
	
	This function is jus a wrapper around most excellent Python dateutils.parser, which allows
	you to provide dates in a free-form manner.  You don't have to specify a string to tell
	this function what your date/time value looks like, you can simply provide the information
	as a text string and it will figure out the rest.  
	
	See http://labix.org/python-dateutil#head-1443e0f14ad5dff07efd465e080d1110920673d8-2 for
	examples of the different date and time formats that can be used.  It could be anything from
	"12/31/1999" to "Today is 25 of September of 2003, exactly at 10:49:41 with timezone -03:00."
	
	If you really want to provide a specific format, use the parse_formatted_temporal() 
	or temporal_from_parts() functions instead.  However, this function is much easier to use.
	
	Parameters:
	
		input_text, a string containing a datetime value.
	
	Returns:
	
		A python datetime (if called from Python) or a string containing an ISO formatted date time (if called from Pig).
	
	'''
	return parser.parse(input_text, fuzzy=True)


def temporal_from_parts(year=1970,month=1,day=1,hour=0,minute=0,second=0,microsecond=0):
	'''
	Returns a datetime from the parts provided.  Default value is the epoch of '1970-01-01 00:00:00'
	You cannot make an invalid datetime with this function.
	
	Parameters:

		year: an integer, defaults to 1970
		month, an integer [1-12], defaults to 1
		day, an integer [1-31], defaults to 1
		hour, an integer [0-23], defaults to 0
		minute, an integer [0-59], defaults to 0
		second, an integer [0-59], defaults to 0
			
	Returns:
	
		A python datetime (if called from Python) or a string containing an ISO formatted date time (if called from Pig).

	'''
	
	return str(datetime(year,month,day,hour,minute,second,microsecond))


def parse_formatted_temporal(input_text, input_format="%Y-%m-%d %H:%M:%S"):
	'''
	Returns a datetime from the string.  You cannot make an invalid datetime with this function.
	
	Parameters:

		input_text: an integer, defaults to 1970
		input_format: a text string that can be used by Python to interpret input_text properly.
		
	NOTE:
	
		For more dtails on input_format, see the Python documentation for the strptime function at
		http://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
			
	Returns:
	
		A python datetime (if called from Python) or a string containing an ISO formatted date time (if called from Pig).


	'''	
	return datetime.strptime(input_text, input_format)


def date_name(date_part, input_text):
	'''
	Returns a character string representing the specific part of the specified datetime.  While
	it primarily parts out logic to other functions, everything returned from this function will
	be formatted as a character string, making for more consistent handling.

	Notes:
	
		(1) This is a useful function seen in T-SQL. See http://msdn.microsoft.com/en-us/library/ms174395.aspx for more details.

	Parameters:

		date_part: a text string containing an english name of the part that should be returned. This could be 
		something like 'year' or 'hour' or an abbreviation like 'wk' or 'qq' (see notes for a full list).
		input_text: a string with a datetime value.
				
	Returns:
	
		A string containing the particular part of the datetime (if valid) or an empty string (if invalid).

	'''
	
	if date_part == 'year' or date_part == 'yy' or date_part == 'yyyy':
		return str(year(input_text))
	elif date_part == 'quarter' or date_part == 'qq' or date_part == 'q':
		return str(quarter(input_text))
	elif date_part == 'month' or date_part == 'mm' or date_part == 'm':
		return str(month(input_text))
	elif date_part == 'day_of_year' or date_part == 'dy' or date_part == 'y':
		return str(day_of_year(input_text))
	elif date_part == 'day_name' or date_part == 'dn':
		return str(day_name(input_text))
	elif date_part == 'day' or date_part == 'dd' or date_part == 'd':
		return str(day(input_text))
	elif date_part == 'week' or date_part == 'wk' or date_part == 'ww':
		return str(week(input_text))
	elif date_part == 'weekday' or date_part == 'dw':
		return weekday(input_text)
	elif date_part == 'day_of_week' or date_part == 'dow':
		return str(day_of_week(input_text))	
	elif date_part == 'hour' or date_part == 'hh':
		return str(hour(input_text))
	elif date_part == 'minute' or date_part == 'mi' or date_part == 'n':
		return str(minute(input_text))
	elif date_part == 'second' or date_part == 'ss' or date_part == 's':
		return str(second(input_text))
	elif date_part == 'microsecond' or date_part == 'mcs':
		return str(microsoecond(input_text))
	elif date_part == 'epoch' or date_part == 'unix' or date_part == 'ep':
		return str(epoch(input_text))
	elif date_part == 'TZoffset' or date_part == 'tz':
		return tz_offset(input_text)
	elif date_part == 'ISO_WEEK' or date_part == 'iso_wk' or date_part == 'isoww':
		return str(iso_week(input_text))
	else:
		return ''


def date_diff(date_part, start_text, end_text):
	'''
	Returns a character string representing the specific part of the difference between two specified datetimes.  While
	it primarily parts out logic to other functions, everything returned from this function will
	be formatted as a character string, making for more consistent handling.

	Notes:
	
		(1) This is a useful function seen in T-SQL. See http://msdn.microsoft.com/en-us/library/ms189794.aspx for more details.


	Parameters:

		date_part: a text string containing an english name of the part that should be returned. This could be 
		something like 'year' or 'hour' or an abbreviation like 'wk' or 'qq' (see notes for a full list).
		start_text: a string with a datetime value representing the start of the period.
		end_text: a string with a datetime value representing the end of the period..
				
	Returns:
	
		A string containing the particular part of the datetime (if valid) or an empty string (if invalid).

	'''
	
	start_dt = parse_temporal(start_text)
	end_dt = parse_temporal(end_text)

	# this should be a positive number
	delta = relativedelta(parse_temporal(end_text), parse_temporal(start_text))

	if date_part == 'year' or date_part == 'yy' or date_part == 'yyyy':
		return str(delta.years)
	elif date_part == 'quarter' or date_part == 'qq' or date_part == 'q':			
		return (((end_dt.years * 4) + (4-quarter(end_text))) - ((start_dt.years * 4) + (4-quarter(start_text))))
	elif date_part == 'month' or date_part == 'mm' or date_part == 'm':
		return str(delta.months)
	elif date_part == 'day' or date_part == 'dd' or date_part == 'd':
		return str(delta.days)
	elif date_part == 'week' or date_part == 'wk' or date_part == 'ww':
		return str(delta.weeks)
	elif date_part == 'hour' or date_part == 'hh':
		return str(delta.hours)
	elif date_part == 'minute' or date_part == 'mi' or date_part == 'n':
		return str(delta.minutes)
	elif date_part == 'second' or date_part == 'ss' or date_part == 's':
		return str(delta.seconds)
	elif date_part == 'microsecond' or date_part == 'mcs':
		return str(delta.microseconds)
	else:
		return ''


def date_add(date_part, number, input_text):
	'''
	Returns a character string representing the the datetime with a certain number of units added to it. 

	Notes:
	
		(1) This is a useful function seen in T-SQL. See http://msdn.microsoft.com/en-us/library/ms189794.aspx for more details.

	Parameters:

		date_part: a text string containing an english name of the part that should be returned. This could be 
		something like 'year' or 'hour' or an abbreviation like 'wk' or 'qq' (see notes for a full list).
		number: the number to add to the datetime.  This can be positive or negative.
		input_text: a string with a datetime value representing the temporal value to be manipulated.
				
	Returns:
	
		A string containing the particular part of the datetime (if valid) or an empty string (if invalid).

	'''	
	dt = parse_temporal(input_text)
	
	if date_part == 'year' or date_part == 'yy' or date_part == 'yyyy':
		return str(dt+relativedelta(years=+number))
	elif date_part == 'quarter' or date_part == 'qq' or date_part == 'q':			
		return str(dt+relativedelta(months=+(3*number)))
	elif date_part == 'month' or date_part == 'mm' or date_part == 'm':
		return str(dt+relativedelta(months=+number))
	elif date_part == 'week' or date_part == 'wk' or date_part == 'ww':
		return str(dt+relativedelta(weeks=+number))
	elif date_part == 'day' or date_part == 'dd' or date_part == 'd':
		return str(dt+relativedelta(days=+number))
	elif date_part == 'hour' or date_part == 'hh':
		return str(dt+relativedelta(hours=+number))
	elif date_part == 'minute' or date_part == 'mi' or date_part == 'n':
		return str(dt+relativedelta(minutes=+number))
	elif date_part == 'second' or date_part == 'ss' or date_part == 's':
		return str(dt+relativedelta(seconds=+number))
	elif date_part == 'microsecond' or date_part == 'mcs':
		return str(dt+relativedelta(microseconds=+number))
	else:
		return ''


def date_trunc(date_part, input_text):
	'''
	Truncates a datetime down to the unit specified. All precision below the date_part will be removed.

	Notes:
	
		(1) This is a useful function seen in Oracle, Postgres, and others. See http://msdn.microsoft.com/en-us/library/ms189794.aspx for more details.

	Parameters:

		date_part: a text string containing an english name of the part that should be returned. This could be 
		something like 'year' or 'hour' or an abbreviation like 'wk' or 'qq' (see notes for a full list).
		input_text: a string with a datetime value representing the temporal value to be manipulated.
				
	Returns:
	
		A string containing the particular part of the datetime (if valid) or an empty string (if invalid).

	'''	
	
	input_dt = parse_temporal(input_text)

	if date_part == 'year' or date_part == 'yy' or date_part == 'yyyy':
		return str(input_dt.replace(month=1,day=1,hour=0, minute=0, second=0, microsecond=0))
	elif date_part == 'quarter' or date_part == 'qq' or date_part == 'q':			
		return str(input_dt.replace(month=(3*((quarter(str(input_dt)))-1))+1,day=1,hour=0, minute=0, second=0, microsecond=0))
	elif date_part == 'month' or date_part == 'mm' or date_part == 'm':
		return str(input_dt.replace(day=1,hour=0, minute=0, second=0, microsecond=0))
	elif date_part == 'day' or date_part == 'dd' or date_part == 'd':
		return str(input_dt.replace(hour=0, minute=0, second=0, microsecond=0))
	elif date_part == 'hour' or date_part == 'hh':
		return str(input_dt.replace(minute=0, second=0, microsecond=0))
	elif date_part == 'minute' or date_part == 'mi' or date_part == 'n':
		return str(input_dt.replace(second=0, microsecond=0))
	elif date_part == 'second' or date_part == 'ss' or date_part == 's':
		return str(input_dt.replace(microsecond=0))
	else:
		return ''	


def date_start_of(date_part, input_text):
	'''
	A synonym for the date_trunc function.
	
	Parameters:

		date_part: a text string containing an english name of the part that should be returned. This could be 
		something like 'year' or 'hour' or an abbreviation like 'wk' or 'qq' (see notes for a full list).
		input_text: a string with a datetime value representing the temporal value to be manipulated.
	
	Returns:
		
		A string containing the particular part of the datetime (if valid) or an empty string (if invalid).

	'''
	
	return date_trunc(date_part, input_text)


def date_end_of(date_part, input_text):
	'''
	Returns a datetime representing the last second of the time period containing the datetime specified.  

	Notes:

		(1) This can be combined nicely with the date_start_of function to isolate a range of values.
	
	Parameters:

		date_part: a text string containing an english name of the part that should be returned. This could be 
		something like 'year' or 'hour' or an abbreviation like 'wk' or 'qq' (see notes for a full list).
		input_text: a string with a datetime value representing the temporal value to be manipulated.
				
	Returns:
	
		A string containing the particular part of the datetime (if valid) or an empty string (if invalid).

	'''	
	
	input_dt = parse_temporal(input_text)

	if date_part == 'year' or date_part == 'yy' or date_part == 'yyyy':
		return date_add('second', -1, str(date_trunc('quarter', str(input_dt+relativedelta(months=+12)))))
	elif date_part == 'quarter' or date_part == 'qq' or date_part == 'q':			
		return date_add('second', -1, str(date_trunc('quarter', str(input_dt+relativedelta(months=+3)))))
	elif date_part == 'month' or date_part == 'mm' or date_part == 'm':
		return date_add('second', -1, str(date_trunc('quarter', str(input_dt+relativedelta(months=+1)))))
	elif date_part == 'day' or date_part == 'dd' or date_part == 'd':
		return date_add('second', -1, str(date_trunc('day', str(input_dt+relativedelta(days=+1)))))
	elif date_part == 'hour' or date_part == 'hh':
		return date_add('second', -1, str(date_trunc('day', str(input_dt+relativedelta(hours=+1)))))
	elif date_part == 'minute' or date_part == 'mi' or date_part == 'n':
		return date_add('second', -1, str(date_trunc('day', str(input_dt+relativedelta(minutes=+1)))))
	else:
		return ''		

				

def today():
	'''
	A function to return today's date.  This can be combined nicely with date_add for ranges.

	Parameters:
				
	Returns:
	
		A string containing todays date.

	'''	
	
	return str(date.today())



def now():
	'''
	A function to return the current date and time.  This can be combined nicely with date_add for ranges.

	Parameters:
				
	Returns:
	
		A string containing the date and time when this function was called.

	'''	
	
	return str(datetime.now())

		

def year(input_text):
	'''
	Get the year for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the year.
	'''	

	return parse_temporal(input_text).year
	

def month(input_text):
	'''
	Get the month for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the month (1-12).
	'''	
	
	return int(parse_temporal(input_text).month)
	

def day_of_year(input_text):
	'''
	Get the day of the year for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the day of the year (1-366).
	'''	
	
	return parse_temporal(input_text).timetuple().tm_yday


def day(input_text):
	'''
	Get the day for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the day (1-31).
	'''	
	
	return int(parse_temporal(input_text).day)


def quarter(input_text):
	'''
	Get the quarter for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the quarter (1-4).
	'''	

	return (month(input_text)-1)//3 + 1


def week(input_text):
	'''
	Get the week of the year for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the week (1-52).
	'''	
	
	return int(parse_temporal(input_text).strftime("%W"))


def iso_week(input_text):
	'''
	Get the ISO defined week of the year for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the week (1-53).
	'''	
	
	return int(parse_temporal(input_text).isocalendar()[1])


def day_name(input_text):
	'''
	Get the day of the week as text for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An string with the text name of the day of the week.
	'''	
	
	return parse_temporal(input_text).strftime("%A")


def day_of_week(input_text):
	'''
	Get the numeric day of the week for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the week (1-7).
	'''	
	
	return int(parse_temporal(input_text).timetuple().tm_wday)+1


def hour(input_text):
	'''
	Get the numeric hour of the day for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the hour (0-23).
	'''	
	
	return int(parse_temporal(input_text).hour)


def minute(input_text):
	'''
	Get the numeric minute of the hour for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the hour (0-59).
	'''	
	
	return int(parse_temporal(input_text).minute)


def second(input_text):
	'''
	Get the numeric second of the minute for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the second (0-59).
	'''		
	return int(parse_temporal(input_text).second)


def microsecond(input_text):
	'''
	Get the numeric microsoecond for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An integer with the hour (0-1000000).
	'''		
	return long(parse_temporal(input_text).microsecond)


def tz_offset(input_text):
	'''
	Get the timezone offset for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		An string with the offset
	'''		
	return parse_temporal(input_text).strftime("%Z")

'''
Returns the number of seconds since the epoch from this datetime.  This ported from Python.

:param input_text:	text string with temporal values to be parsed and used.
:return	long
'''

def epoch(input_text):
	'''
	Get the UNIX datetime for a particular temporal value.

	Parameters:
	
		input_text: a string with a datetime value representing the temporal value to be manipulated.
			
	Returns:
	
		A long with the number of seconds.
	'''		
	
	return long((parse_temporal(input_text) - datetime(1970,1,1)).seconds)
