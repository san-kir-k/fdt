#!/bin/bash

cd ./aos2soa
for length in 10000 100000 1000000 10000000
do
    sed -e "s/100000/$length/g" static_raw_n10.gen.cpp > static_raw_l$length.gen.cpp
done

cd ../soa2aos
for length in 10000 100000 1000000 10000000
do
    sed -e "s/100000/$length/g" static_raw_n10.gen.cpp > static_raw_l$length.gen.cpp
done

cd ..