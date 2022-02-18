package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractAggregationCollector<T extends SentinelHelloCollector> implements SentinelHelloCollector {

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
        resetCheckResult();
    }

    protected synchronized int collectHello(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();
        checkFinishedInstance.add(info.getHostPort());
        if (context.isSuccess()) checkResult.addAll(context.getResult());
        else checkFailInstance.add(info.getHostPort());

        return checkFinishedInstance.size();
    }

    protected synchronized void handleAllBackupDcHellos(RedisHealthCheckInstance instance) {
        Set<SentinelHello> hellos = new HashSet<>(checkResult);
        resetCheckResult();
        this.realCollector.onAction(new SentinelActionContext(instance, hellos));
    }

    protected void handleAllActiveDcHellos(RedisHealthCheckInstance instance, Set<SentinelHello> hellos) {
        this.realCollector.onAction(new SentinelActionContext(instance, hellos));
    }

    protected synchronized void resetCheckResult() {
        this.checkFinishedInstance.clear();
        this.checkFailInstance.clear();
        this.checkResult.clear();
    }

}
