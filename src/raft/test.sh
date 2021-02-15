#!/bin/bash

for i in {1..20}
do
	time go test -race -run 2A & # Put a function in the background
done
 
## Put all cust_func in the background and bash 
## would wait until those are completed 
## before displaying all done message
wait