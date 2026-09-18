package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.redis.checker.RelationsService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;

/**
 * @author chen.zhu
 * <p>
 * Aug 30, 2018
 */
public class ProxyEnabledHealthCheckConfig extends AbstractHealthCheckConfig {

    public ProxyEnabledHealthCheckConfig(CheckerConfig checkerConfig, RelationsService relationsService) {
        super(checkerConfig, relationsService);
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
