#!/bin/bash

#yum -y install perl
#
#datadir=`echo ${HDFS_CONF_dfs_journalnode_data_dir} | perl -pe 's#file://##'`

source ~/.bashrc

echo "HADOOP_HOME=${HADOOP_HOME}"
echo "JAVA_HOME=${JAVA_HOME}"
echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"

#if [ ! -d $datadir ]; then
#  echo "Journal node data directory not found: $datadir"
#  exit 2
#fi

echo "contents of /hadoop"
ls /hadoop

echo "contents of /hadoop/data"
ls /hadoop/data

echo "contents of /hadoop/data/journalnode"
ls /hadoop/data/journalnode

echo "contents of /hadoop/data/journalnode/${CLUSTER_NAME}"
ls /hadoop/data/journalnode/${CLUSTER_NAME}

mkdir -p /hadoop/data/journalnode/${CLUSTER_NAME}

echo "contents of /hadoop/data/journalnode/${CLUSTER_NAME}"
ls /hadoop/data/journalnode/${CLUSTER_NAME}

echo "starting journal node..."
${HADOOP_PREFIX}/bin/hdfs --config ${HADOOP_CONF_DIR} journalnode
echo "Journal node started!"