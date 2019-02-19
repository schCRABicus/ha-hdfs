FROM ha-hdfs:0.1
#FROM centos:7
#FROM ubuntu:latest
MAINTAINER Mikalai Lushchytski <Mikalai_Lushchytski@epam.com>

# https://issues.apache.org/jira/browse/HDFS-9427
HEALTHCHECK CMD curl -f http://localhost:8480/ || exit 1

ARG CLUSTER_NAME
ARG ACTIVE_NAME_NODE
ARG STANDBY_NAME_NODE
ARG JOURNAL_NODE_1
ARG JOURNAL_NODE_2
ARG JOURNAL_NODE_3
ARG ZK_NODE_1
ARG ZK_NODE_2
ARG ZK_NODE_3

ENV HDFS_CONF_dfs_journalnode_name_dir=/hadoop/data/journalnode
#ENV USER=root
#RUN mkdir -p /hadoop/data/journalnode
RUN mkdir -p /hadoop/data/journalnode/${CLUSTER_NAME}
#RUN chown -R hadoop /hadoop/data/journalnode/${CLUSTER_NAME}
RUN ls /hadoop
RUN ls /hadoop/data
RUN ls /hadoop/data/journalnode
RUN ls /hadoop/data/journalnode/${CLUSTER_NAME}

#VOLUME /home/hadoop/data/dataNode

ADD /conf/conf-setup.sh /conf-setup.sh
ADD /journalnode/run.sh /run.sh
RUN chmod a+x /conf-setup.sh
RUN chmod a+x /run.sh

RUN echo "CLUSTER_NAME = ${CLUSTER_NAME}"
RUN echo "ACTIVE_NAME_NODE = ${ACTIVE_NAME_NODE}"
RUN /conf-setup.sh

EXPOSE 8485
EXPOSE 8480

#ENV HADOOP_PREFIX=/hadoop/hadoop-3.2.0
#ENV HADOOP_CONF_DIR=/hadoop/hadoop-3.2.0/etc/hadoop
#ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/

CMD ["/run.sh"]