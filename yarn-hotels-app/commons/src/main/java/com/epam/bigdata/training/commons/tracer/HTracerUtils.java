package com.epam.bigdata.training.commons.tracer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

/**
 * Hadoop tracer utility wrapper
 */
public class HTracerUtils {

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
}
