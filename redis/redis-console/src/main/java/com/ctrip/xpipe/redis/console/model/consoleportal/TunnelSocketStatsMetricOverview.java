package com.ctrip.xpipe.redis.console.model.consoleportal;

import java.util.List;

public class TunnelSocketStatsMetricOverview {

    private List<TunnelSocketStatsMetric> frontend;

    private List<TunnelSocketStatsMetric> backend;

    public TunnelSocketStatsMetricOverview(List<TunnelSocketStatsMetric> frontend,
                                           List<TunnelSocketStatsMetric> backend) {
        this.frontend = frontend;
        this.backend = backend;
    }

    public List<TunnelSocketStatsMetric> getFrontend() {
        return frontend;
    }

    public List<TunnelSocketStatsMetric> getBackend() {
        return backend;
    }
}
