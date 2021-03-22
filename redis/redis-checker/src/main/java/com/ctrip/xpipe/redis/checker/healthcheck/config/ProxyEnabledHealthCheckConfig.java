package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.redis.checker.config.CheckerConfig;

/**
 * @author chen.zhu
 * <p>
 * Aug 30, 2018
 */
public class ProxyEnabledHealthCheckConfig extends AbstractHealthCheckConfig {

    public ProxyEnabledHealthCheckConfig(CheckerConfig checkerConfig) {
        super(checkerConfig);
    }

    @Override
    public int delayDownAfterMilli() {
        return checkerConfig.getDownAfterCheckNumsThroughProxy() * checkerConfig.getHealthyDelayMilliThroughProxy();
    }

    @Override
    public int pingDownAfterMilli() {
        return checkerConfig.getPingDownAfterMilliThroughProxy();
    }

    @Override
    public int getHealthyDelayMilli() {
        return checkerConfig.getHealthyDelayMilliThroughProxy();
    }
}
