FROM ha-hdfs:0.1
#FROM centos:7
#FROM ubuntu:latest
MAINTAINER Mikalai Lushchytski <Mikalai_Lushchytski@epam.com>

# https://issues.apache.org/jira/browse/HDFS-9427
HEALTHCHECK CMD curl -f http://localhost:9864/ || exit 1

ARG CLUSTER_NAME
ARG ACTIVE_NAME_NODE
ARG STANDBY_NAME_NODE
ARG JOURNAL_NODE_1
ARG JOURNAL_NODE_2
ARG JOURNAL_NODE_3
ARG ZK_NODE_1
ARG ZK_NODE_2
ARG ZK_NODE_3

ENV HDFS_CONF_dfs_datanode_name_dir=/hadoop/data/datanode
RUN mkdir -p /hadoop/data/datanode
#RUN chown -R hadoop /hadoop/data/datanode
#VOLUME /home/hadoop/data/dataNode

ADD /conf/conf-setup.sh /conf-setup.sh
ADD /datanode/run.sh /run.sh
RUN chmod a+x /conf-setup.sh
RUN chmod a+x /run.sh

RUN /conf-setup.sh
#ENV USER=root

EXPOSE 9864
#
#ENV HADOOP_PREFIX=/hadoop/hadoop-3.2.0
#ENV HADOOP_CONF_DIR=/hadoop/hadoop-3.2.0/etc/hadoop
#ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/

CMD ["/run.sh"]