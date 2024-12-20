#!/bin/bash

. ./utils.sh

testInit /data/cbj/destor
CONFIG=-p"working-directory $WORKING_DIR"
# SRC_DIR=/data/datasets/rdb
SRC_DIR=$1

cd ~/destor
remake

[[ "$*" =~ "-chk" ]] && chk=1 || chk=0
[[ "$*" =~ "-bkp" ]] && bkp=1 || bkp=0

set -ex
if [[ $bkp == 1 ]]; then

    resetAll

    set -x
    # basic test
    mkdir -p ${DST_DIR}${RESTORE_ID}
    ./destor ${SRC_DIR} "${CONFIG}" > ${LOG_DIR}/0.log

    if [ $chk -eq 1 ]; then
        ./destor -r0 ${DST_DIR}${RESTORE_ID} "${CONFIG}"
    fi

fi

let ++RESTORE_ID

function update() {
    # update test
    rm -f ${WORKING_DIR}/container.pool_new
    rm -f ${WORKING_DIR}/recipes/bv1*
    # rm -f ${WORKING_DIR}/upgrade_external_cache
    rm -f ${WORKING_DIR}/kvstore_file
    rm -rf ${WORKING_DIR}/rocksdb0
    cat ~/.ssh/passwd | sudo -S ./flush.sh
    
    ./destor -u0 ${SRC_DIR} -i"$1" "${CONFIG}" > ${LOG_DIR}/$1.log
    echo $?

    if [ $chk -eq 1 ]; then
        mkdir -p ${DST_DIR}${RESTORE_ID}
        # rm ${WORKING_DIR}/container.pool
        ./destor -n1 ${DST_DIR}${RESTORE_ID} "${CONFIG}"
    fi

    let ++RESTORE_ID
}

# update 0
# update 2
# update 3
# update 4
# update 5

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
