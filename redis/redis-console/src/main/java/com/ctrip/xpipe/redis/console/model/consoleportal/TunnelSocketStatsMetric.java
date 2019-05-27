package com.ctrip.xpipe.redis.console.model.consoleportal;

public class TunnelSocketStatsMetric {

    private String metricType;

    private double value;

    public TunnelSocketStatsMetric(String metricType, double value) {
        this.metricType = metricType;
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    public String getMetricType() {
        return metricType;
    }
}
