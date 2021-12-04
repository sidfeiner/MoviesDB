#!/bin/sh

kaggleDataDir=$1

python3 ./KaggleLoader.py load \
--mysql-usr XXX \
--mysql-pwd XXX \
--mysql-host XXX
--mysql-db XXX
--movies-file-path $kaggleDataDir/movies_metadata.csv \
--credits-file-path $kaggleDataDir/credits.csv \
--keywords-file-path $kaggleDataDir/keywords.csv \
--log-level DEBUG