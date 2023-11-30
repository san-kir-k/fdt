#!/bin/bash

cd ./aos2soa
for fields_num in 1 10 100 1000
do
    python3 static_generator.py -n $fields_num
done

cd ../soa2aos
for fields_num in 1 10 100 1000
do
    python3 static_generator.py -n $fields_num
done

cd ..
