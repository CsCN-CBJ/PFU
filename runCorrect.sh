#!/bin/bash

. ./utils.sh

testInit

cd ~/destor
remake

resetAll
genBasicData

# basic test
mkdir -p ${DST_DIR}${RESTORE_ID}
destor ${SRC_DIR}
destor -r0 ${DST_DIR}${RESTORE_ID}
let ++RESTORE_ID

# update test
mkdir -p ${DST_DIR}${RESTORE_ID}
destor -u0 ${SRC_DIR}
destor -n1 ${DST_DIR}${RESTORE_ID}
let ++RESTORE_ID

compareRestore

if [ ${flag} -eq 0 ]; then
    make clean -s
    echo "all test passed"
else
    echo "test failed"
    exit 1
fi
