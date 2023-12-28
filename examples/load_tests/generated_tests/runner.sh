#!/bin/bash

cd ..

echo "---------- MEMSET ------------"
echo ">>> Running memset etalon <<<"
echo
./memset
echo
echo "Done..."
echo "------------------------------"
echo

echo "----------- STATIC -------------"
echo ">>> Running wide tests <<<"
echo
for fields_num in 1 10 100 1000
do
    echo "Columns: $fields_num"
    ./soa2aos_wide_static_$fields_num
    ./aos2soa_wide_static_$fields_num
    echo
done
echo
echo "Done..."
echo

echo ">>> Running long tests <<<"
echo
for length in 10000 100000 1000000 10000000
do
    echo "Length: $length"
    ./soa2aos_long_static_$length
    ./aos2soa_long_static_$length
    echo
done
echo
echo "Done..."
echo "-------------------------------"

echo "----------- ARROW -------------"
echo ">>> Running wide tests <<<"
echo
for fields_num in 1 10 100 1000
do
    echo "Columns: $fields_num"
    ./soa2aos_wide_arrow_$fields_num
    ./aos2soa_wide_arrow_$fields_num
    echo
done
echo
echo "Done..."
echo

echo ">>> Running long tests <<<"
echo
for length in 10000 100000 1000000 10000000
do
    echo "Length: $length"
    ./soa2aos_long_arrow_$length
    ./aos2soa_long_arrow_$length
    echo
done
echo
echo "Done..."
echo "------------------------------"