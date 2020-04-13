#!/bin/bash

# 1 get parameter
parameter_number=$#

if [[ ${parameter_number}==0 ]]; then
    echo no parameter
    exit
fi

# 2 get file name
parameter_1=$1
file_name=`basename ${parameter_1}`
echo file_name=${file_name}

# 3 get path
path=`cd -P $(dirname "$parameter_1"); pwd `
echo path=${path}

# 4 user name
user=`whoami`

# 5 cycle

for (( host = 100; host < 104; host++ )); do
    echo ------------------------ hadoop${host} ------------------------------
    rsync -av ${path}/${file_name} ${user}@hadoop${host}:${path}
done