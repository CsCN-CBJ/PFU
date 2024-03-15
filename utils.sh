#!/bin/bash

function testInit() {
    set -e

    TEST_DIR=~/destor/temp
    WORKING_DIR=${TEST_DIR}/working
    SRC_DIR=${TEST_DIR}/src
    DST_DIR=${TEST_DIR}/dst
    RESTORE_ID=0
    LOG_DIR=${TEST_DIR}/log

    rm -f ~/local/bin/destor*
}

function remake() {
    make clean -s
    make -s
    make install -s
}

function resetAll() {
    rm -f destor.log
    rm -rf ${TEST_DIR}
    mkdir -p ${WORKING_DIR}/recipes/
    mkdir -p ${WORKING_DIR}/index/
    mkdir -p ${SRC_DIR}
    mkdir -p ${LOG_DIR}
}

function resetWorkingDir() {
    rm -rf ${WORKING_DIR}
    mkdir -p ${WORKING_DIR}/recipes/
    mkdir -p ${WORKING_DIR}/index/
}

function genBasicData() {
    dd if=/dev/urandom of=${SRC_DIR}/a bs=1M count=100
    dd if=/dev/urandom of=${SRC_DIR}/b bs=1M count=100
    cp ${SRC_DIR}/a ${SRC_DIR}/c
}

function compareRestore() {
    # 判断各个restore的文件是否和源文件一致, define $flag
    set +e  # 防止diff错误导致程序退出, 需要比较完所有的文件
    flag=0
    for ((i=0; i<RESTORE_ID; i++))
    do
        diff -sqr ${SRC_DIR} ${DST_DIR}${i} > compare.txt
        [ $? -ne 0 ] && flag=1
    done
    set -e
}

