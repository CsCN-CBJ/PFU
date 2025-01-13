#!/bin/bash

# $1: upgrade level
# $2: test directory
# $3: configuration string

if [ $# -ne 3 ]; then
    echo "Error: Missing argument"
    exit 1
fi

set -ex

LEVEL=$1
TEST_DIR=$2
WORKING_DIR=${TEST_DIR}/working
LOG_DIR=${TEST_DIR}/log

CONFIG=-p"working-directory $WORKING_DIR, $3"

# update test
mkdir -p ./data/
rm -f ${WORKING_DIR}/container.pool_new
rm -f ${WORKING_DIR}/recipes/bv1*
rm -f ${WORKING_DIR}/kvstore_file
# rm -f ${WORKING_DIR}/upgrade_external_cache
# rm -rf ${WORKING_DIR}/rocksdb0
cat ~/.ssh/passwd | sudo -S ./flush.sh
sleep 20

./destor-cbj-test-special -u0 /dev/null -i"$LEVEL" "${CONFIG}" > ${LOG_DIR}/$1.log

rm -f ${WORKING_DIR}/container.pool_new
rm -f ${WORKING_DIR}/recipes/bv1*
