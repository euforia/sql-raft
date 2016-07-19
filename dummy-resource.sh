#!/bin/bash

tag=$1
i=0;
while [ 1 ]; do 
	echo $(date) $tag $i
	i=`expr $i \+ 1`
	sleep 4;
done