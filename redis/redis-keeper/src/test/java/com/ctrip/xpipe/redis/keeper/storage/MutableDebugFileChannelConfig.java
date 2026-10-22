package com.ctrip.xpipe.redis.keeper.storage;

/** Test config whose fields are changed directly; {@code failure} makes every load throw. */
final class MutableDebugFileChannelConfig implements DebugFileChannelConfig {

    volatile boolean eio;
    volatile long delayMilli;
    volatile String ops = FileFaultInjector.ALL_OPS;
    volatile String pathContains = "";
    volatile RuntimeException failure;

    @Override
    public boolean isEio() {
        RuntimeException f = failure;
        if (f != null) {
            throw f;
        }
        return eio;
    }

    @Override
    public long getDelayMilli() {
        return delayMilli;
    }

    @Override
    public String getOps() {
        return ops;
    }

    @Override
    public String getPathContains() {
        return pathContains;
    }
}
