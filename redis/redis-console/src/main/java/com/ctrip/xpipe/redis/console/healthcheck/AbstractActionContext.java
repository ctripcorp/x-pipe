package com.ctrip.xpipe.redis.console.healthcheck;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class AbstractActionContext<C> implements ActionContext<C> {

    protected C c;

    private long nanoTimestamp;

    protected RedisHealthCheckInstance instance;

    public AbstractActionContext(RedisHealthCheckInstance instance, C c) {
        this.instance = instance;
        this.c = c;
        this.nanoTimestamp = System.nanoTime();
    }

    @Override
    public RedisHealthCheckInstance instance() {
        return instance;
    }

    @Override
    public C getResult() {
        return c;
    }

    @Override
    public long nanoTimestamp() {
        return nanoTimestamp;
    }
}
