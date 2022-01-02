#!/bin/bash

currentDir=$(dirname "$0")
cd "$currentDir" || exit

export MYSQL_USER=xxx
export MYSQL_PWD=yyy
python3 ./server.py