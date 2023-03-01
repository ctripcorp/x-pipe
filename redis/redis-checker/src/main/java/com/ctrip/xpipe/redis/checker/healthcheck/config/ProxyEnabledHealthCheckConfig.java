package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;

/**
 * @author chen.zhu
 * <p>
 * Aug 30, 2018
 */
public class ProxyEnabledHealthCheckConfig extends AbstractHealthCheckConfig {

    public ProxyEnabledHealthCheckConfig(CheckerConfig checkerConfig, DcRelationsService dcRelationsService) {
        super(checkerConfig, dcRelationsService);
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

    @Override
    public int downAfterCheckNums() {
        return checkerConfig.getDownAfterCheckNumsThroughProxy();
    }
}
