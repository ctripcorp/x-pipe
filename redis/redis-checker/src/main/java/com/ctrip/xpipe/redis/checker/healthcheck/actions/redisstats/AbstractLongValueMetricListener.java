package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats;

import com.ctrip.xpipe.redis.checker.healthcheck.*;

public abstract class AbstractLongValueMetricListener<T extends ActionContext<Long, RedisHealthCheckInstance>>
        extends AbstractMetricListener<T, HealthCheckAction> implements HealthCheckActionListener<T, HealthCheckAction> {

    protected abstract String getMetricType();

    @Override
    public void onAction(T context) {
        Long value = context.getResult();

        long recvTimeMilli = context.getRecvTimeMilli();
        RedisInstanceInfo info = context.instance().getCheckInfo();
        tryWriteMetric(getPoint(getMetricType(), value, recvTimeMilli, info));
    }

}
