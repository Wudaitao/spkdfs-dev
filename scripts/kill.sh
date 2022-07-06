#!/bin/sh
kill -9 $(pidof datanode)
kill -9 $(pidof namenode)
SCRIPT_DIR=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)
fuser -k ${SCRIPT_DIR}/namenode
fuser -k ${SCRIPT_DIR}/datanode
