#!/usr/bin/env bash

#!/usr/bin/env bash
HADOOP_VERSION=${HADOOP_VERSION:-"3.2.0"}
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

echo " >> Step 1: Install Java"
yum update
yum -y install rsync
yum -y install java-1.8.0-openjdk-devel
yum -y install wget
echo " >> Step 1 - Install Java - complete"

echo " >> Step 2: Download and unpack hadoop distribution"
cd
wget http://ftp.byfly.by/pub/apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
tar -xzf hadoop-${HADOOP_VERSION}.tar.gz
mv hadoop-${HADOOP_VERSION} /hadoop
HDP_HOME=/hadoop/hadoop-${HADOOP_VERSION}
echo " >> Step 2 - hadoop dist of version ${HADOOP_VERSION} downloaded and unpacked"

echo " >> Step 3: add Hadoop binaries to environment variables"
echo "PATH=/hadoop/bin:/hadoop/sbin:$PATH" >> /${HDP_HOME}/.profile
echo " >> Step 3 - add Hadoop binaries to environment variables - complete"

echo " >> Step 4: configure core-site.xml"
addProperty /${HDP_HOME}/etc/hadoop/core-site.xml fs.default.name hdfs://${CLUSTER_NAME}
addProperty /${HDP_HOME}/etc/hadoop/core-site.xml dfs.journalnode.edits.dir /hadoop/data/journalnode
echo " >> Step 4: core-site.xml configured"

echo " >> Step 5: setting paths for HDFS"
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.name.dir" "/hadoop/data/namenode"
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.datanode.data.dir" "/hadoop/data/datanode"
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.replication" 2
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.permissions" false
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.nameservices" CLUSTER_NAME
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.ha.namenodes.${CLUSTER_NAME}" "active,standby"
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.rpc-address.ha-cluster.active" "${ACTIVE_NAME_NODE}:9000"
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.rpc-address.ha-cluster.standby" "${STANDBY_NAME_NODE}:9000"
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.http-address.ha-cluster.active" "${ACTIVE_NAME_NODE}:9870"
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.http-address.ha-cluster.standby" "${STANDBY_NAME_NODE}:9870"
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.namenode.shared.edits.dir" "qjournal://${JOURNAL_NODE_1}:8485;${JOURNAL_NODE_1}:8485;${JOURNAL_NODE_1}:8485/${CLUSTER_NAME}"
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.client.failover.proxy.provider.${CLUSTER_NAME}" "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "dfs.ha.automatic-failover.enabled" true
addProperty /${HDP_HOME}/etc/hadoop/hdfs-site.xml "ha.zookeeper.quorum" "${ZK_NODE_1}:2181,${ZK_NODE_2}:2181,${ZK_NODE_3}:2181"
echo " >> Step 5 - setting paths for HDFS - complete"

echo " >> Step 6: update .bashrc"
cat > ~/.bashrc << EOM
export HADOOP_HOME=${HDP_HOME}
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_PREFIX=${HDP_HOME}
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/
export PATH=$PATH: $JAVA_HOME/bin: $HADOOP_HOME/bin: $HADOOP_HOME/sbin
EOM
source ~/.bashrc
echo " >> Step 6 - update .bashrc - complete"

echo "HADOOP_HOME=${HADOOP_HOME}"
echo "JAVA_HOME=${JAVA_HOME}"
echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"

echo "Done basic installation"