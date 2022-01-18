#!/usr/bin/env bash
export COMPSTOR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -d $COMPSTOR/lib ]; then
    if [ -z $LD_LIBRARY_PATH ]; then
        export LD_LIBRARY_PATH=$COMPSTOR/lib
    else
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$COMPSTOR/lib
    fi
fi
