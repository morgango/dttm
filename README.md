DTTM
==========================

DTTM (short for "Date/Time") is a UDF library that aims to make date manipulation simpler and more robust for unstructured data.  Conceptually, the date/time UDFs in Hadoop are fine, but they seem to be designed to be easy to code, and not particularly easy to use for end users.  

Core Design Principles
--------------------------

The core design principles of the DTTM library are to: 

1. Allow date and time manipulations from *unstructured text*, which I refer to as **temporal values**.  DTTM does not requiring specific formatting for dates or times, instead it tries to figure out what is in a text string and use it.  Formatting temporals to fit the language is ass-backward, and typically this is a time consuming, difficult and error-prone part of data preparation. A **temporal value** could be anything from "09/25/2003 10:49:41 -03:00" to "Today is 25 of September of 2003, exactly at 10:49:41 with timezone -03:00." These should both be usable and interchangeable *without user intervention*.
	
2. **Make temporal manipulations very easy to do**. So easy that an analyst could understand them without needing 
much (if any) additional knowledge about data types.
	
3. **Make date and time functions that are friendly to users.**  That means having fewer functions with parameters instead of lots of very similar functions.  Also, include useful utility functions that allow common date manipulations, such as extracting parts of temporal values (for partitioning) and finding the beginning and end of days, months, and quarters.
	
4. **Make date and time manipulations more durable.**  If formatting is slightly different for you shouldn't have to do a lot of work to compensate.    If your data looks like it could be a datetime to the eyeball, you should be able to use it for data processing.

5. **Leverage the best APIs from procedural and query based languages** and not reinvent the wheel.  Use existing code whenever possible and take advantage of platform-specific niceties.  Elements of Python, Transact-SQL, and PL/SQL were leveraged  for back- and front-end work.

Usage
--------------------------

Much of DTTM is designed to which is to have a single function with parameters to specify how work should be done.  For example, there is one function called date_diff that has a parameter (datepart) that is used to tell what units should be returned.  This is much easier fo end users to remember and use than something like one function per unit (date_diff_seconds, date_diff_minutes, date_diff_hours, etc.).
	
In actual operation, the functions that get used the most are: 

* parse_temporal(), which is used implicitly used by every other function.
* Manipulation functions (date_add(), date_diff(), date_trunc(), date_start_of(), date_end_of(), date_name()) functions.
* today() and now() 

There is a common set of string constants that are used across all of the manipulation functions through the datepart variable:
	
	'year', 'yy', 'yyyy': The numeric calendar year
	'quarter', 'qq', 'q': The numeric calendar quarter (1-4)
	'month', 'mm', 'm':* The numeric calendar month (1-12)
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
	
All values may not be able to be used in each function, as they may or may not make sense.  For example, it does not make sense to truncate a temporal value to the microsecond, as this is the lowest increment of time used with temporal values. 
	
Notes
--------------------------

1. Internally, DTTM is done in Python, and would first be implemented in Pig using Jython.  I understand that these could be done using Java, but the weight of the language is simply too much for me to bear for what should be simple functionality.

2. When using DTTM functions, you can simply specify your temporal values in a string without a particular format.  The library is set up so that it will try to convert any text to a proper datetime value, using the totally awesome Python dateutil library and parser.  A temporal value could be anything from "12/31/1999" to "Today is 25 of September of 2003, exactly at 10:49:41 with timezone -03:00." and it would be usable.
	
3. This library is initially intended for PIG, but may be extended to other Hadoop projects as opportunites arise.

4. As mentioned before, this library depends on the python datetime and dateutil libraries.  They must be installed and available in your Jython Path across your cluster.
