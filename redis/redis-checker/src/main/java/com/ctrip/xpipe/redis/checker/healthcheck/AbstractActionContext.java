package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public abstract class AbstractActionContext<C, T extends HealthCheckInstance> implements ActionContext<C, T> {

    protected C c;

    private long recvTimeMilli;

    protected T instance;

    protected Throwable cause;

    public AbstractActionContext(T instance, C c) {
        this.instance = instance;
        this.c = c;
        this.cause = null;
        this.recvTimeMilli = System.currentTimeMillis();
    }

    public AbstractActionContext(T instance, Throwable t) {
        this.instance = instance;
        this.c = null;
        this.cause = t;
        this.recvTimeMilli = System.currentTimeMillis();
    }

    @Override
    public T instance() {
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
    public boolean isSuccess() {
        return null == cause;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(instance, c);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractActionContext<?,?> that = (AbstractActionContext<?,?>) o;
        return recvTimeMilli == that.recvTimeMilli &&
                Objects.equals(c, that.c) &&
                Objects.equals(cause, that.cause) &&
                Objects.equals(instance, that.instance);
    }
}
