#!/usr/bin/env bash

: "${CLUSTER_NAME:?Cluster name must be provided via CLUSTER_NAME env variable}"
: "${ACTIVE_NAME_NODE:?Active Name Node address must be provided via ACTIVE_NAME_NODE env variable}"
: "${STANDBY_NAME_NODE:?StandBy Name Node address must be provided via STANDBY_NAME_NODE env variable}"
: "${JOURNAL_NODE_1:?Journal Node 1 address must be provided via JOURNAL_NODE_1 env variable}"
: "${JOURNAL_NODE_2:?Journal Node 2 address must be provided via JOURNAL_NODE_2 env variable}"
: "${JOURNAL_NODE_3:?Journal Node 3 address must be provided via JOURNAL_NODE_3 env variable}"
: "${ZK_NODE_1:?Zk Node 1 address must be provided via ZK_NODE_1 env variable}"
: "${ZK_NODE_2:?Zk Node 2 address must be provided via ZK_NODE_2 env variable}"
: "${ZK_NODE_3:?Zk Node 3 address must be provided via ZK_NODE_3 env variable}"

function addProperty() {
  local path=$1
  local name=$2
  local value=$3

  local entry="<property><name>$name</name><value>${value}</value></property>"
  local escapedEntry=$(echo $entry | sed 's/\//\\\//g')
  sed -i "/<\/configuration>/ s/.*/${escapedEntry}\n&/" $path
}

source ~/.bashrc

echo "HADOOP_HOME=${HADOOP_HOME}"
echo "JAVA_HOME=${JAVA_HOME}"
echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"

echo "ls hadoop home:"
ls $HADOOP_HOME

echo " >> Step 1: configure core-site.xml"
addProperty ${HADOOP_HOME}/etc/hadoop/core-site.xml fs.defaultFS hdfs://${CLUSTER_NAME}
addProperty ${HADOOP_HOME}/etc/hadoop/core-site.xml dfs.journalnode.edits.dir /hadoop/data/journalnode
echo " >> Step 1: core-site.xml configured"

echo " >> Step 2: setting paths for HDFS"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.name.dir" "/hadoop/data/namenode"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.datanode.data.dir" "/hadoop/data/datanode"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.replication" 2
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.permissions" false
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.nameservices" ${CLUSTER_NAME}
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.ha.namenodes.${CLUSTER_NAME}" "active,standby"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.rpc-address.${CLUSTER_NAME}.active" "${ACTIVE_NAME_NODE}:8020"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.rpc-address.${CLUSTER_NAME}.standby" "${STANDBY_NAME_NODE}:8020"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.http-address.${CLUSTER_NAME}.active" "${ACTIVE_NAME_NODE}:9870"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.http-address.${CLUSTER_NAME}.standby" "${STANDBY_NAME_NODE}:9870"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.shared.edits.dir" "qjournal://${JOURNAL_NODE_1}:8485;${JOURNAL_NODE_2}:8485;${JOURNAL_NODE_3}:8485/${CLUSTER_NAME}"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.client.failover.proxy.provider.${CLUSTER_NAME}" "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.journalnode.edits.dir" "/hadoop/data/journalnode/edits"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.ha.automatic-failover.enabled" true
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "dfs.ha.fencing.methods" "shell(/bin/true)"
addProperty ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml "ha.zookeeper.quorum" "${ZK_NODE_1}:2181,${ZK_NODE_2}:2181,${ZK_NODE_3}:2181"
echo " >> Step 2 - setting paths for HDFS - complete"
