passwd = LOAD 'passwd' USING PigStorage(':') AS (
		user:CHARARRAY, 
		passwd:CHARARRAY, 
		uid:INT, 
		gid:INT, 
		userinfo:CHARARRAY, 
		home:CHARARRAY, 
		shell:CHARARRAY);

-- lets see what we have in the set.
--DUMP passwd; 

-- group together everything we have in the same shell.  We 
grp_shell = GROUP passwd BY shell;  

-- lets see what we have in the set.
--DUMP grp_shell;

DESCRIBE grp_shell;

grp_shell_min = FILTER grp_shell BY group == '/sbin/nologin';

DUMP grp_shell_min;
