package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.SentinelHelloCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractAggregationCollector<T extends SentinelHelloCollector> implements SentinelHelloCollector {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private T realCollector;

    protected Set<HostPort> checkFinishedInstance = new HashSet<>();

    protected Set<HostPort> checkFailInstance = new HashSet<>();

    protected Set<SentinelHello> checkResult = new HashSet<>();

    protected final String clusterId;

    protected final String shardId;

    public AbstractAggregationCollector(T sentinelHelloCollector, String clusterId, String shardId) {
        this.realCollector = sentinelHelloCollector;
        this.clusterId = clusterId;
        this.shardId = shardId;
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        // no nothing
    }

    protected synchronized void collectHello(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getRedisInstanceInfo();
        checkFinishedInstance.add(info.getHostPort());
        if (!context.isFail()) checkResult.addAll(context.getResult());
        else checkFailInstance.add(info.getHostPort());
    }

    protected void handleAllHello(RedisHealthCheckInstance instance) {
        Set<SentinelHello> hellos = new HashSet<>(checkResult);
        resetCheckResult();
        this.realCollector.onAction(new SentinelActionContext(instance, hellos));
    }

    protected synchronized void resetCheckResult() {
        this.checkFinishedInstance.clear();
        this.checkFailInstance.clear();
        this.checkResult.clear();
    }

}
