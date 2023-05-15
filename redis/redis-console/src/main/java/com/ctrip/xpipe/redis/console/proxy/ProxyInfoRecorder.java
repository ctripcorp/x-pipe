package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.SessionTrafficResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelTrafficResult;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@Lazy
public class ProxyInfoRecorder implements ProxyMonitorCollector.Listener {

    private Logger logger = LoggerFactory.getLogger(ProxyInfoRecorder.class);

    private ExecutorService executors = Executors.newSingleThreadExecutor(XpipeThreadFactory.create(getClass().getSimpleName()));

    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    private static final String PING_METRIC_TYPE = "proxy.ping";

    private static final String TRAFFIC_METRIC_TYPE = "proxy.traffic";

    private static final String FAKE_CLUSTER = "cluster", FAKE_SHARD = "shard";

    @Override
    public void ackPingStatsResult(ProxyMonitorCollector collector, List<PingStatsResult> realTimeResults) {
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                reportPings(collector, realTimeResults);
            }
        });
    }

    @Override
    public void ackTrafficStatsResult(ProxyMonitorCollector collector, List<TunnelTrafficResult> realTimeResults) {
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                reportTraffic(collector, realTimeResults);
            }
        });
    }

    private void reportPings(ProxyMonitorCollector collector, List<PingStatsResult> realTimeResults) {
        if(realTimeResults == null || realTimeResults.isEmpty()) {
            logger.debug("[report] null result for PingStatsResult");
            return;
        }
        for(PingStatsResult pingStatsResult : realTimeResults) {
            try {
                getMetricProxy().writeBinMultiDataPoint(getMetricData(collector, pingStatsResult));
            } catch (MetricProxyException e) {
                logger.error("[report]", e);
            }
        }
    }

    private void reportTraffic(ProxyMonitorCollector collector, List<TunnelTrafficResult> realTimeResults) {
        if(realTimeResults == null || realTimeResults.isEmpty()) {
            logger.debug("[report] null result for TrafficStatsResult");
            return;
        }
        DataFlowCounter counter = getDataFlowCount(realTimeResults);
        reportTraffics(collector, counter);
        logDcTraffics(collector, counter);
    }

    private MetricData getMetricData(ProxyMonitorCollector collector, PingStatsResult pingStatsResult) {
        MetricData metricData = new MetricData(PING_METRIC_TYPE, collector.getProxyInfo().getDcName(), FAKE_CLUSTER, FAKE_SHARD);
        metricData.addTag("srcproxy", collector.getProxyInfo().getHostPort().getHost());
        metricData.setHostPort(pingStatsResult.getReal());
        metricData.setTimestampMilli(pingStatsResult.getStart());
        metricData.setValue(pingStatsResult.getEnd() - pingStatsResult.getStart());
        return metricData;
    }

    private void logDcTraffics(ProxyMonitorCollector collector, DataFlowCounter counter) {
        EventMonitor.DEFAULT.logEvent(String.format("%s.%s", TRAFFIC_METRIC_TYPE, collector.getProxyInfo().getDcName()),
                "frontend.input", counter.getFrontendInput());
        EventMonitor.DEFAULT.logEvent(String.format("%s.%s", TRAFFIC_METRIC_TYPE, collector.getProxyInfo().getDcName()),
                "frontend.output", counter.getFrontendOutput());
        EventMonitor.DEFAULT.logEvent(String.format("%s.%s", TRAFFIC_METRIC_TYPE, collector.getProxyInfo().getDcName()),
                "backend.input", counter.getBackendInput());
        EventMonitor.DEFAULT.logEvent(String.format("%s.%s", TRAFFIC_METRIC_TYPE, collector.getProxyInfo().getDcName()),
                "backend.output", counter.getBackendOutput());
    }

    private void reportTraffics(ProxyMonitorCollector collector, DataFlowCounter counter) {
        try {
            getMetricProxy().writeBinMultiDataPoint(getMetricData(collector, "frontend.input", counter.getFrontendInput()));
            getMetricProxy().writeBinMultiDataPoint(getMetricData(collector, "frontend.output", counter.getFrontendOutput()));
            getMetricProxy().writeBinMultiDataPoint(getMetricData(collector, "backend.input", counter.getBackendInput()));
            getMetricProxy().writeBinMultiDataPoint(getMetricData(collector, "backend.output", counter.getBackendOutput()));
        } catch (MetricProxyException e) {
            logger.error("[report]", e);
        }
    }

    private DataFlowCounter getDataFlowCount(List<TunnelTrafficResult> trafficResults) {
        long frontendInput = 0, frontendOutput = 0, backendInput = 0, backendOutput = 0;
        for(TunnelTrafficResult tunnelTrafficResult : trafficResults) {
            SessionTrafficResult frontend = tunnelTrafficResult.getFrontend();
            SessionTrafficResult backend = tunnelTrafficResult.getBackend();
            frontendInput += frontend.getInputRates();
            frontendOutput += frontend.getOutputRates();
            backendInput += backend.getInputRates();
            backendOutput += backend.getOutputRates();
        }
        return new DataFlowCounter(frontendInput, frontendOutput, backendInput, backendOutput);
    }

    private static final String TRAFFIC_DIRECTION_TAG = "direction";

    private MetricData getMetricData(ProxyMonitorCollector collector, String direction, long val) {
        MetricData metricData = new MetricData(TRAFFIC_METRIC_TYPE, collector.getProxyInfo().getDcName(), FAKE_CLUSTER, FAKE_SHARD);
        metricData.setHostPort(collector.getProxyInfo().getHostPort());
        metricData.addTag(TRAFFIC_DIRECTION_TAG, direction);
        metricData.setTimestampMilli(System.currentTimeMillis());
        metricData.setValue(val);
        return metricData;
    }

    protected MetricProxy getMetricProxy() {
        return this.metricProxy;
    }

    private class DataFlowCounter {

        private final long frontendInput, frontendOutput, backendInput, backendOutput;

        public DataFlowCounter(long frontendInput, long frontendOutput, long backendInput, long backendOutput) {
            this.frontendInput = frontendInput;
            this.frontendOutput = frontendOutput;
            this.backendInput = backendInput;
            this.backendOutput = backendOutput;
        }

        public long getFrontendInput() {
            return frontendInput;
        }

        public long getFrontendOutput() {
            return frontendOutput;
        }

        public long getBackendInput() {
            return backendInput;
        }

        public long getBackendOutput() {
            return backendOutput;
        }
    }
}
