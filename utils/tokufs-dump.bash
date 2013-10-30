#!/bin/bash

echo 'DATA DUMP'
./tokudb_dump -x -h $1 data $2
echo 'DONE DATA DUMP'
echo ''
echo 'META DUMP'
./tokudb_dump -m -x -h $1 meta $2
echo 'DONE META DUMP'
