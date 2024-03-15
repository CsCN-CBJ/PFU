#!/bin/bash

. ./utils.sh

testInit

resetAll
genBasicData

cd ~/destor
remake
mkdir -p ${DST_DIR}${RESTORE_ID}
destor ${SRC_DIR} > ${LOG_DIR}/0.log
destor -r0 ${DST_DIR}${RESTORE_ID} > /dev/null
new=`hexdump -n 8 -e '8/1 "%02x" "\n"' ${WORKING_DIR}/container.pool`
let ++RESTORE_ID

cd ~/destor0
remake
resetWorkingDir
mkdir -p ${DST_DIR}${RESTORE_ID}
destor ${SRC_DIR} > ${LOG_DIR}/1.log
destor -r0 ${DST_DIR}${RESTORE_ID} > /dev/null
old=`hexdump -n 8 -e '8/1 "%02x" "\n"' ${WORKING_DIR}/container.pool`
let ++RESTORE_ID

compareRestore

# 去掉最后7行的速度信息
head -n -7 ${LOG_DIR}/0.log | sort > ${LOG_DIR}/0.log.sorted
head -n -7 ${LOG_DIR}/1.log | sort > ${LOG_DIR}/1.log.sorted
diff -sqr ${LOG_DIR}/0.log.sorted ${LOG_DIR}/1.log.sorted > compareLog.txt
[ $? -ne 0 ] && flag=1

echo $new $old $flag
if [ "$new" == "$old" ] && [ "$flag" == "0" ]; then
    make clean -s
    echo "all test passed"
else
    echo "test failed"
    exit 1
fi
