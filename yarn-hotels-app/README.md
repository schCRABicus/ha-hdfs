## Application launch manual

To launch the application, first package all three jars - client, application master and application - and copy them to hadoop sandbox.
```
mvn clean package && docker cp yarn-hotels-app/yarn-application-master/target/yarn-application-master-1.0-SNAPSHOT.jar sandbox-hdp:yarn-application-master.jar && docker cp yarn-hotels-app/yarn-client/target/yarn-client-1.0-SNAPSHOT.jar sandbox-hdp:yarn-client.jar && docker cp yarn-hotels-app/yarn-hotels-application/target/yarn-hotels-application-1.0-SNAPSHOT.jar sandbox-hdp:yarn-hotels-application.jar
```

Once done, invoke the following command from inside the sandbox 
(for me, resource manager port was not available to outside that;s why I was not able to execute the yarn client from local machine):
```
java -XX:+PrintGCDetails \
    -XX:+PrintGCDateStamps \
    -Xloggc:yarn-client-gc-logs.txt \
    -cp yarn-client.jar com.epam.bigdata.training.client.ClientLauncher   \
    --appname HotelsYarnApplication  \
    --default_fs hdfs://sandbox-hdp.hortonworks.com:8020 \
    --rm_address sandbox-hdp.hortonworks.com:8050 \
    --jar yarn-application-master.jar  \
    --main_class com.epam.bigdata.training.appmaster.ApplicationMasterLauncher  \
    --app_jar yarn-hotels-application.jar  \
    --app_main_class com.epam.bigdata.training.app.ApplicationLauncher  \
    --app_input_path hdfs://sandbox-hdp.hortonworks.com:8020/HotelsYarnApplication/test.csv \
    --app_output_path hdfs://sandbox-hdp.hortonworks.com:8020/HotelsYarnApplication/output_result.csv \
    --container_memory 512  \
    --container_vcores 1  \
    --master_memory 256  \
    --master_vcores 1  \
    --num_containers 1
```

For bigger dataset:
```
java -XX:+PrintGCDetails \
    -XX:+PrintGCDateStamps \
    -Xloggc:yarn-client-gc-logs.txt \
    -cp yarn-client.jar com.epam.bigdata.training.client.ClientLauncher   \
    --appname HotelsYarnApplication  \
    --default_fs hdfs://sandbox-hdp.hortonworks.com:8020 \
    --rm_address sandbox-hdp.hortonworks.com:8050 \
    --jar yarn-application-master.jar  \
    --main_class com.epam.bigdata.training.appmaster.ApplicationMasterLauncher  \
    --app_jar yarn-hotels-application.jar  \
    --app_main_class com.epam.bigdata.training.app.ApplicationLauncher  \
    --app_input_path hdfs://sandbox-hdp.hortonworks.com:8020/HotelsYarnApplication/train.csv \
    --app_output_path hdfs://sandbox-hdp.hortonworks.com:8020/HotelsYarnApplication/output_train.csv \
    --container_memory 512  \
    --container_vcores 1  \
    --master_memory 256  \
    --master_vcores 1  \
    --num_containers 2
```

## Zipkin metrics aggregation

To start yarn application tracing, the following steps must be complete:

1. Zipkin server with scribe enabled should be started. This can be done via
```
docker run --name=zipkin -d -e SCRIBE_ENABLED=true -p 9411:9411 -p 9410:9410 openzipkin/zipkin
```
command.

To make sipkin server visible from sandbox hdp, zipkin has to be connected to hdp network:
```
docker network connect cda zipkin
```

2. The next step is to provide the necessary span receiver configurations
to yarn application - 
```java
yarnConfiguration.set("span.receiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver");
yarnConfiguration.set("sampler.classes", "AlwaysSampler");
yarnConfiguration.set("hadoop.htrace.zipkin.scribe.hostname", "zipkin");
yarnConfiguration.set("hadoop.htrace.zipkin.scribe.port", "9410");
```

The first 2 specify span receiver and sampling mode (always), 
the latter two are zipkin specific configurations - the location of span aggregator.

In addition to that, htrace-zipkin library must be added to classpath: 
```xml
            <dependency>
                <groupId>org.apache.htrace</groupId>
                <artifactId>htrace-zipkin</artifactId>
                <version>4.1.0-incubating</version>
            </dependency>
```
3. Once configured, the target operations can be traced the following way:
```java
    /**
     *
     * @param name          Tracer name
     * @param scope         Trace scope like read, write, acquire client, etc.
     * @param configuration Hadoop configuration whish should contain zipkin server settings and tracer config.
     *                      Minimum required settings are:
     *                      <ul>
     *                          <li>span.receiver.classes</li>
     *                          <li>sampler.classes</li>
     *                          <li>hadoop.htrace.zipkin.scribe.hostname</li>
     *                          <li>hadoop.htrace.zipkin.scribe.port</li>
     *                      </ul>
     * @param operation     Operation to trace.
     */
    public static void trace(String name, String scope, Configuration configuration, Runnable operation) {
        try (Tracer tracer = new Tracer.Builder(name).
                conf(TraceUtils.wrapHadoopConf("", configuration)).
                build();
             TraceScope ts = tracer.newScope(scope)) {

            operation.run();
        }
    }
```
