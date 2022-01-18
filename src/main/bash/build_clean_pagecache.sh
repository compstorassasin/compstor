#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

mkdir -p $COMPSTOR/target/bin
BIN_PATH=$COMPSTOR/target/bin/drop_pagecache

g++ $DIR/../cpp/drop_pagecache.cc -O3 -o $BIN_PATH

sudo chown root $BIN_PATH
sudo chmod u+s $BIN_PATH
