package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.redis.console.model.consoleportal.TunnelSocketStatsMetric;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;

public interface TunnelSocketStatsAnalyzer {

    List<FrontendAndBackendMetrics> analyze(ProxyChain chain);

    Pair<TunnelSocketStatsMetric, TunnelSocketStatsMetric> analyze(TunnelSocketStatsResult result);

    String getType();

    void reportMetrics(List<FrontendAndBackendMetrics> metrics);

    public static class FrontendAndBackendMetrics extends Pair<MetricData, MetricData> {

        public FrontendAndBackendMetrics(MetricData frontend, MetricData backend) {
            super(frontend, backend);
        }

        public MetricData getFrontend() {
            return getKey();
        }

        public MetricData getBackend() {
            return getValue();
        }
    }
}
