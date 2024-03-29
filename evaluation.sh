#!/bin/bash

. ./utils.sh

testInit /data/cbj/destor
CONFIG=-p"working-directory $WORKING_DIR"
SRC_DIR=/data/cbj/temp

cd ~/destor
remake

resetAll

set -x
# basic test
mkdir -p ${DST_DIR}${RESTORE_ID}
destor ${SRC_DIR} "${CONFIG}" > ${LOG_DIR}/${RESTORE_ID}.log
cp -r ${WORKING_DIR} ${WORKING_DIR}_bak

destor -r0 ${DST_DIR}${RESTORE_ID} "${CONFIG}"
let ++RESTORE_ID

function update() {
    # update test
    rm -r ${WORKING_DIR}
    cp -r ${WORKING_DIR}_bak ${WORKING_DIR}
    
    mkdir -p ${DST_DIR}${RESTORE_ID}
    destor -u0 ${SRC_DIR} -i"$1" "${CONFIG}" > ${LOG_DIR}/${RESTORE_ID}.log
    rm ${WORKING_DIR}/container.pool
    destor -n1 ${DST_DIR}${RESTORE_ID} "${CONFIG}"

    let ++RESTORE_ID
}

for ((i=0; i<3; i++)); do
    update $i
done

compareRestore

if [ ${flag} -eq 0 ]; then
    make clean -s
    echo "all test passed"
else
    echo "test failed"
    exit 1
fi
