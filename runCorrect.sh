#!/bin/bash

set -e

cd ~/destor
# make clean
make
make install

TEST_DIR=~/destor/temp
WORKING_DIR=${TEST_DIR}/working
SRC_DIR=${TEST_DIR}/src
DST_DIR=${TEST_DIR}/dst

JOB_ID=0
RESTORE_ID=0

rm destor.log
rm -r ${TEST_DIR}
mkdir -p ${WORKING_DIR}/recipes/
mkdir -p ${WORKING_DIR}/index/
mkdir -p ${SRC_DIR}

# generate data
dd if=/dev/urandom of=${SRC_DIR}/a bs=1M count=10
dd if=/dev/urandom of=${SRC_DIR}/b bs=1M count=10
cp ${SRC_DIR}/a ${SRC_DIR}/c

# basic test
mkdir -p ${DST_DIR}${RESTORE_ID}
destor ${SRC_DIR}
destor -r0 ${DST_DIR}${RESTORE_ID}
let ++RESTORE_ID

# update test1
mkdir -p ${DST_DIR}${RESTORE_ID}
destor -u0 ${SRC_DIR}
destor -n1 ${DST_DIR}${RESTORE_ID}
let ++RESTORE_ID

# 判断各个restore的文件是否和源文件一致
set -x
set +e  # 防止diff错误导致程序退出, 需要比较完所有的文件
flag=0
for ((i=0; i<RESTORE_ID; i++))
do
diff -sr ${SRC_DIR} ${DST_DIR}${i}
[ $? -ne 0 ] && flag=1
done

if [ ${flag} -eq 0 ]; then
    make clean
    echo "all test passed"
else
    echo "test failed"
    exit 1
fi
