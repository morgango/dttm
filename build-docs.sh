#!/bin/bash

if [ ! -d tmp ]; 
then
	mkdir tmp
fi

cat dttm.py | sed 's/@outputSchema.*//g' > tmp/dttm.py

cd tmp

pydoc -w dttm

cp dttm.html ..

cd ..

open dttm.html

