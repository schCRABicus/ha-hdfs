#!/bin/bash

namedir=`echo ${HDFS_CONF_dfs_namenode_name_dir}`

source ~/.bashrc

if [ -z "${CLUSTER_NAME}" ]; then
  echo "Cluster name not specified"
  exit 2
fi

function waitUntilJournalNodeAvailable() {
  local host=$1

  until $(curl --output /dev/null --silent --head --fail http://${host}:8480); do
    echo " >> Journal node ${host} not available, waiting for 5 more seconds"
    sleep 5
  done
}

echo "WAITING UNTIL for journal nodes to become available..."
waitUntilJournalNodeAvailable ${JOURNAL_NODE_1}
waitUntilJournalNodeAvailable ${JOURNAL_NODE_2}
waitUntilJournalNodeAvailable ${JOURNAL_NODE_3}

if [[ ! -z "${IS_STANDBY_NODE}" ]]; then

  echo "Formatiting and starting failover controller"
  ${HADOOP_PREFIX}/bin/hdfs zkfc -formatZK -force -nonInteractive
  ${HADOOP_PREFIX}/bin/hdfs --config ${HADOOP_CONF_DIR} --daemon start zkfc

  echo "Starting StandBy NameNode..."
  ${HADOOP_PREFIX}/bin/hdfs --config ${HADOOP_CONF_DIR} namenode -bootstrapStandby -force
  ${HADOOP_PREFIX}/bin/hdfs --config ${HADOOP_CONF_DIR} namenode
  echo "StandBy NameNode started!"
fi

if [[ ! -z "${IS_ACTIVE_NODE}" ]]; then
  if [ "`ls -A $namedir`" == "" ]; then
    echo "Formatting namenode name directory: $namedir"
    ${HADOOP_PREFIX}/bin/hdfs --config ${HADOOP_CONF_DIR} namenode -format ${CLUSTER_NAME} -force -nonInteractive
  fi

  echo "Formatiting and starting failover controller"
  ${HADOOP_PREFIX}/bin/hdfs zkfc -formatZK -force -nonInteractive
  ${HADOOP_PREFIX}/bin/hdfs --config ${HADOOP_CONF_DIR} --daemon start zkfc

  echo "Starting Active NameNode..."
  ${HADOOP_PREFIX}/bin/hdfs --config ${HADOOP_CONF_DIR} namenode
  echo "Active NameNode started!"
fi

if [[ -z "${IS_STANDBY_NODE}" ]] && [[ -z "${IS_ACTIVE_NODE}" ]]; then
    echo "either IS_ACTIVE_NODE or IS_STANDBY_NODE env variable must be provided to identify NameNode state"
    exit 2
fi