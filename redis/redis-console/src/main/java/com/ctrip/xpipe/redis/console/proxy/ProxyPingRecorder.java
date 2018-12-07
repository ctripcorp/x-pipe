package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;
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
public class ProxyPingRecorder implements ProxyMonitorCollector.Listener {

    private Logger logger = LoggerFactory.getLogger(ProxyPingRecorder.class);

    private ExecutorService executors = Executors.newSingleThreadExecutor(XpipeThreadFactory.create(getClass().getSimpleName()));

    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    private static final String METRIC_TYPE = "proxy.ping";

    private static final String FAKE_CLUSTER = "cluster", FAKE_SHARD = "shard", FAKE_DC = "dc";

    @Override
    public void ackPingStatsResult(List<PingStatsResult> realTimeResults) {
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                report(realTimeResults);
            }
        });


    }

    private void report(List<PingStatsResult> realTimeResults) {
        if(realTimeResults == null || realTimeResults.isEmpty()) {
            logger.warn("[report] null result for PingStatsResult");
            return;
        }
        for(PingStatsResult pingStatsResult : realTimeResults) {
            try {
                getMetricProxy().writeBinMultiDataPoint(getMetricData(pingStatsResult));
            } catch (MetricProxyException e) {
                logger.error("[report]", e);
            }
        }
    }

    private MetricData getMetricData(PingStatsResult pingStatsResult) {
        MetricData metricData = new MetricData(METRIC_TYPE, FAKE_DC, FAKE_CLUSTER, FAKE_SHARD);
        metricData.setHostPort(pingStatsResult.getReal());
        metricData.setTimestampMilli(pingStatsResult.getStart());
        metricData.setValue(pingStatsResult.getEnd() - pingStatsResult.getStart());
        return metricData;
    }

    protected MetricProxy getMetricProxy() {
        return this.metricProxy;
    }
}
