package com.ctrip.xpipe.redis.checker.healthcheck.config;

import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;

/**
 * @author chen.zhu
 * <p>
 * Aug 30, 2018
 */
public class DefaultHealthCheckConfig extends AbstractHealthCheckConfig {
    public DefaultHealthCheckConfig(CheckerConfig checkerConfig, DcRelationsService dcRelationsService) {
        super(checkerConfig, dcRelationsService);
    }
}
