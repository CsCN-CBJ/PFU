#!/bin/bash
# 比较现阶段的destor与老版本的destor在20字节backup时的行为是否完全一致

. ./utils.sh

testInit
CONFIG=-p"log-level debug"

resetAll
genBasicData

cd ~/destor
remake
mkdir -p ${DST_DIR}${RESTORE_ID}
destor ${SRC_DIR} "${CONFIG}" > ${LOG_DIR}/0.log
destor -r0 ${DST_DIR}${RESTORE_ID} > /dev/null
new=`hexdump -n 8 -e '8/1 "%02x" "\n"' ${WORKING_DIR}/container.pool`
let ++RESTORE_ID

cd ~/destor0
remake
resetWorkingDir
mkdir -p ${DST_DIR}${RESTORE_ID}
destor ${SRC_DIR} "${CONFIG}" > ${LOG_DIR}/1.log
destor -r0 ${DST_DIR}${RESTORE_ID} > /dev/null
old=`hexdump -n 8 -e '8/1 "%02x" "\n"' ${WORKING_DIR}/container.pool`
let ++RESTORE_ID

compareRestore

# 去掉一些会产生差异的信息
function logFilter() {
    grep -v "MB/s" "$1" \
    | grep -v "Filter phase: write a segment start at offset" \
    | grep -v "job type:"\
    | grep -v "The index buffer is ready for more chunks"\
    | grep -v "total time"\
    | sort > "$1.sorted"
}
logFilter ${LOG_DIR}/0.log
logFilter ${LOG_DIR}/1.log

set +e
diff -s ${LOG_DIR}/0.log.sorted ${LOG_DIR}/1.log.sorted >> ${LOG_DIR}/compareLog.txt

echo $new $old $flag
if [ "$new" == "$old" ] && [ "$flag" == "0" ]; then
    make clean -s
    echo "all test passed"
    echo "please check the comparation result in ${LOG_DIR}/compareLog.txt"
    head -v -n5 ${LOG_DIR}/compareLog.txt
else
    echo "test failed"
    exit 1
fi
