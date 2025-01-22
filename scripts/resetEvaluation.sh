#!/bin/bash

set -ex
cd ~/destor

make clean
make

cd -
cp ~/destor/destor destor-cbj-test-special

mkdir -p ./data/ ./log/
