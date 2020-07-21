package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats;

import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;

public abstract class AbstractLongValueMetricListener<T extends ActionContext<Long>> extends AbstractMetricListener<T> implements HealthCheckActionListener<T> {

    protected abstract String getMetricType();

    @Override
    public void onAction(T context) {
        Long value = context.getResult();

        long recvTimeMilli = context.getRecvTimeMilli();
        RedisInstanceInfo info = context.instance().getRedisInstanceInfo();
        tryWriteMetric(getPoint(getMetricType(), value, recvTimeMilli, info));
    }

}
