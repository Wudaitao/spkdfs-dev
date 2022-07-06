#!/bin/sh
REAL_DFS_HOME=${HOME}/opendfs
echo "REAL_DFS_HOME: ${REAL_DFS_HOME}"

rm -rf ${REAL_DFS_HOME}/data/namenode/editlog/*
rm -rf ${REAL_DFS_HOME}/data/namenode/coredump/*
rm -rf ${REAL_DFS_HOME}/data/namenode/logs/*
rm -rf ${REAL_DFS_HOME}/data/namenode/fsimage/*

rm -rf ${REAL_DFS_HOME}/data/datanode/block/current/*
rm -f ${REAL_DFS_HOME}/data/datanode/persistent_data/grouplist
rm -rf ${REAL_DFS_HOME}/data/datanode/coredump/*
rm -rf ${REAL_DFS_HOME}/data/datanode/logs/*

echo "Y"|./namenode -f