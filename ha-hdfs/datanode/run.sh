#!/bin/bash

#yum -y install perl
#
#datadir=`echo ${HDFS_CONF_dfs_datanode_data_dir} | perl -pe 's#file://##'`

source ~/.bashrc

#if [ ! -d $datadir ]; then
#  echo "Datanode data directory not found: $datadir"
#  exit 2
#fi

#if [ -n "$SLEEP_SECONDS" ]; then
#   sleep $SLEEP_SECONDS
#fi

function waitUntilNameNodeAvailable() {
  local host=$1

  until $(curl --output /dev/null --silent --head --fail http://${host}:9870); do
    echo " >> Name node ${host} not available, waiting for 5 more seconds"
    sleep 5
  done
}

echo "WAITING UNTIL for journal nodes to become available..."
waitUntilNameNodeAvailable ${ACTIVE_NAME_NODE}
waitUntilNameNodeAvailable ${STANDBY_NAME_NODE}

echo "starting datanode..."
${HADOOP_PREFIX}/bin/hdfs --config ${HADOOP_CONF_DIR} datanode
echo "datanode started!"