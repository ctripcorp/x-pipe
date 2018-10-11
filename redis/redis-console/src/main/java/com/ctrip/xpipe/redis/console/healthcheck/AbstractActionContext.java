package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public abstract class AbstractActionContext<C> implements ActionContext<C> {

    protected C c;

    private long recvTimeMilli;

    protected RedisHealthCheckInstance instance;

    public AbstractActionContext(RedisHealthCheckInstance instance, C c) {
        this.instance = instance;
        this.c = c;
        this.recvTimeMilli = System.currentTimeMillis();
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
    public long getRecvTimeMilli() {
        return recvTimeMilli;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(instance, c);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractActionContext<?> that = (AbstractActionContext<?>) o;
        return recvTimeMilli == that.recvTimeMilli &&
                Objects.equals(c, that.c) &&
                Objects.equals(instance, that.instance);
    }
}
