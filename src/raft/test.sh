#!/bin/bash

for i in {1..20}
do
	time go test -race -run 2A &
done

wait