package com.ctrip.xpipe.redis.keeper.applier;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplierConfig {

    private AtomicInteger dropAllowRation = new AtomicInteger(10);

    private AtomicInteger dropAllowKeys = new AtomicInteger(-1);

    private AtomicBoolean useXsync = new AtomicBoolean(false);

    private AtomicBoolean protoChangeAllow = new AtomicBoolean(false);

    public int getDropAllowRation() {
        return dropAllowRation.get();
    }

    public void setDropAllowRation(int ration) {
        this.dropAllowRation.set(Math.min(100, Math.max(-1, ration)));
    }

    public int getDropAllowKeys() {
        return dropAllowKeys.get();
    }

    public void setDropAllowKeys(int keys) {
        this.dropAllowKeys.set(Math.max(-1, keys));
    }

    public boolean getUseXsync() {
        return useXsync.get();
    }

    public void setUseXsync(boolean useXsync) {
        this.useXsync.set(useXsync);
    }

    public void setProtoChangeAllow(boolean protoChangeAllow) {
        this.protoChangeAllow.set(protoChangeAllow);
    }

    public boolean getProtoChangeAllow() {
        return protoChangeAllow.get();
    }
}
