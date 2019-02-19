#!/usr/bin/env bash

#systemctl stop firewalld
#systemctl disable firewalld
#systemctl mask --now firewalld
HADOOP_VERSION=${HADOOP_VERSION:-"3.2.0"}

echo " >> Step 1: Install Java"
yum update
yum -y install java-1.8.0-openjdk-devel
yum -y install wget
echo " >> Step 1 - Install Java - complete"

echo " >> Step 2: Download and unpack hadoop distribution"
cd
wget http://ftp.byfly.by/pub/apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
tar -xzf hadoop-${HADOOP_VERSION}.tar.gz
mv hadoop-${HADOOP_VERSION} /hadoop
HDP_HOME=/hadoop
#HDP_HOME=/hadoop-3.1.1-docker
#echo "ls-$HDP_HOME"
#ls $HDP_HOME
echo " >> Step 2 - hadoop dist of version ${HADOOP_VERSION} downloaded and unpacked"

echo " >> Step 3: add Hadoop binaries to environment variables"
echo "PATH=/hadoop/bin:/hadoop/sbin:$PATH" >> ${HDP_HOME}/.profile
echo " >> Step 3 - add Hadoop binaries to environment variables - complete"

echo " >> Step 4: update .bashrc"
cat > ~/.bashrc << EOM
export HADOOP_HOME=${HDP_HOME}
export HADOOP_MAPRED_HOME=${HDP_HOME}
export HADOOP_COMMON_HOME=${HDP_HOME}
export HADOOP_HDFS_HOME=${HDP_HOME}
export HADOOP_CONF_DIR=${HDP_HOME}/etc/hadoop
export HADOOP_PREFIX=${HDP_HOME}
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export PATH=$PATH:/usr/lib/jvm/java-1.8.0-openjdk/bin:${HDP_HOME}/bin:${HDP_HOME}/sbin
EOM
source ~/.bashrc
echo " >> Step 4 - update .bashrc - complete"

echo "HADOOP_HOME=${HADOOP_HOME}"
echo "JAVA_HOME=${JAVA_HOME}"
echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"

echo "Done basic installation"