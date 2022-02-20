package com.ctrip.xpipe.redis.console.healthcheck.nonredis.sentinelconfig;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractCrossDcIntervalCheck;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class SentinelShardBind extends AbstractCrossDcIntervalCheck {

    @Autowired
    private SentinelBalanceService sentinelBalanceService;

    @Autowired
    private ConsoleConfig config;

    @Override
    protected void doCheck() {
        config.getOuterClusterTypes().forEach(clusterType -> {
            sentinelBalanceService.bindShardAndSentinelsByType(ClusterType.lookup(clusterType));
        });
    }

    @Override
    protected long getIntervalMilli() {
        return config.sentinelBindTimeoutMilli();
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return new ArrayList<>();
    }
}
