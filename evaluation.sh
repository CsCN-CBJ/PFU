#!/bin/bash

. ./utils.sh

testInit /data/cbj/destor
CONFIG=-p"working-directory $WORKING_DIR"
SRC_DIR=/data/cbj/temp

cd ~/destor
remake

[[ "$*" =~ "-chk" ]] && chk=1 || chk=0
[[ "$*" =~ "-bkp" ]] && bkp=1 || bkp=0

set -x
rm -rf ~/destor/log/time*
if [[ $bkp == 1 ]]; then

    resetAll
        
    redis-cli -p 6666 FLUSHALL

    set -x
    # basic test
    mkdir ~/destor/log/time
    mkdir -p ${DST_DIR}${RESTORE_ID}
    ./destor ${SRC_DIR} "${CONFIG}" > ${LOG_DIR}/${RESTORE_ID}.log
    cp -r ${WORKING_DIR} ${WORKING_DIR}_bak

    if [ $chk -eq 1 ]; then
        ./destor -r0 ${DST_DIR}${RESTORE_ID} "${CONFIG}"
    fi
    mv ~/destor/log/time ~/destor/log/time${RESTORE_ID}

fi

let ++RESTORE_ID

function update() {
    # update test
    redis-cli -p 6666 FLUSHALL
    rm -r ${WORKING_DIR}
    cp -r ${WORKING_DIR}_bak ${WORKING_DIR}
    
    mkdir -p ~/destor/log/time
    mkdir -p ${DST_DIR}${RESTORE_ID}
    ./destor -u0 ${SRC_DIR} -i"$1" "${CONFIG}" > ${LOG_DIR}/${RESTORE_ID}.log

    if [ $chk -eq 1 ]; then
        rm ${WORKING_DIR}/container.pool
        ./destor -n1 ${DST_DIR}${RESTORE_ID} "${CONFIG}"
    fi
    mv ~/destor/log/time ~/destor/log/time${RESTORE_ID}

    let ++RESTORE_ID
}

update 0
update 1
update 2

if [ $chk -eq 1 ]; then
    compareRestore
fi

if [ ${flag} -eq 0 ]; then
    make clean -s
    echo "all test passed"
else
    echo "test failed"
    exit 1
fi
