package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author lishanglin
 * date 2022/6/10
 */
public class InstanceComponentWrapper<T extends Lifecycle> extends AbstractInstanceComponent {

    private T inner;

    public InstanceComponentWrapper(T inner) {
        this.inner = inner;
    }

    public T getInner() {
        return inner;
    }

    @Override
    protected void doInitialize() throws Exception {
        inner.initialize();
    }

    @Override
    protected void doStart() throws Exception {
        inner.start();
    }

    @Override
    protected void doStop() throws Exception {
        inner.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        inner.dispose();
    }

    @Override
    public int getOrder() {
        return inner.getOrder();
    }

    @Override
    public String toString() {
        return "InstanceComponentWrapper{" + inner.toString() +'}';
    }
}
