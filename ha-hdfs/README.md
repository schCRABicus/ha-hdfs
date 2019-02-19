## How to install

The HDFS HA cluster setup consists of two major steps:
1. HDFS common image build.
2. Cluster launch.

### HDFS common image build.

According to the requirement, the hdfs image is built on
top of CentOS 7 image with `java-1.8.0-openjdk` and `wget`
additionally installed. 

The image is build by default for hadoop of version
`3.2.0`, however one can override the version via 
`HADOOP_VERSION` variable setup.

The image build script is located under `/hdfs-image`
folder.

So, to build the image locally one should execute the following command:
```
docker build -t ha-hdfs:0.1 /hdfs-image
```
from inside the root folder.

### Cluster launch

Once the image is built, the cluster can be started via
```
docker-compose up --build
```
command.

The cluster consists of the following components:
* 3 zookeeper nodes
* 3 journal nodes
* active namenode
* standby namenode
* 2 datanodes

