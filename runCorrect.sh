#!/bin/bash

. ./utils.sh

if [ $# -ne 1 ]; then
  echo "Error: Missing argument"
  exit 1
fi

testInit
CONFIG=-p"log-level debug"

cd ~/destor
remake

resetAll
genBasicData

# basic test
mkdir -p ${DST_DIR}${RESTORE_ID}
destor ${SRC_DIR} "${CONFIG}" > ${LOG_DIR}/${RESTORE_ID}.log
destor -r0 ${DST_DIR}${RESTORE_ID}
let ++RESTORE_ID

# update test
set -x
mkdir -p ${DST_DIR}${RESTORE_ID}
destor -u0 ${SRC_DIR} -i$1 "${CONFIG}" > ${LOG_DIR}/${RESTORE_ID}.log
rm ${WORKING_DIR}/container.pool
destor -n1 ${DST_DIR}${RESTORE_ID}
let ++RESTORE_ID

compareRestore
checkLog ~/destor/log/backup.log 3 0.3333
checkLog ~/destor/log/update.log 3 0.3333

if [ ${flag} -eq 0 ]; then
    make clean -s
    echo "all test passed"
else
    echo "test failed"
    exit 1
fi
