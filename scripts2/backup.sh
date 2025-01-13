#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Error: Missing argument"
  echo "Usage: $0 <src_dir> <working_dir>"
  echo "src_dir can be a directory or a list of file paths"
  exit 1
fi

. ./utils.sh

testInit $2
CONFIG=-p"working-directory $WORKING_DIR"
SRC_DIR=$1

set -ex
cd ~/destor
remake
resetAll

# basic test
mkdir -p ${DST_DIR}${RESTORE_ID}
./destor ${SRC_DIR} "${CONFIG}" > ${LOG_DIR}/0.log
