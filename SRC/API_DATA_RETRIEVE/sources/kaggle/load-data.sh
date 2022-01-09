#!/bin/sh

kaggleDataDir=$1

python3 ./KaggleLoader.py load \
--mysql-usr DbMysql05 \
--mysql-pwd DbMysql05 \
--mysql-host localhost \
--mysql-port 3305 \
--mysql-db DbMysql05 \
--movies-file-path $kaggleDataDir/movies_metadata.csv \
--credits-file-path $kaggleDataDir/credits.csv \
--keywords-file-path $kaggleDataDir/keywords.csv \
--log-level DEBUG