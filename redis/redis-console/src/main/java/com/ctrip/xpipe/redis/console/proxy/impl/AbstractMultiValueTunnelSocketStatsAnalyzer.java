package com.ctrip.xpipe.redis.console.proxy.impl;

public abstract class AbstractMultiValueTunnelSocketStatsAnalyzer extends AbstractNormalKeyValueTunnelSocketStatsAnalyzer {

    private static final String DEFAULT_SPLITTER = "\\s*/\\s*";

    private String splitter;

    public AbstractMultiValueTunnelSocketStatsAnalyzer(String key) {
        super(key);
        this.splitter = DEFAULT_SPLITTER;
    }

    public AbstractMultiValueTunnelSocketStatsAnalyzer(String key, String splitter) {
        super(key);
        this.splitter = splitter;
    }

    @Override
    protected double getValue(String value) {
        return getValueFromString(value.split(splitter));
    }

    protected abstract double getValueFromString(String[] values);

}
