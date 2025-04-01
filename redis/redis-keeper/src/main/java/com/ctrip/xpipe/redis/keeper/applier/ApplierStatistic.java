package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.concurrent.atomic.AtomicLong;

public class ApplierStatistic {

    private AtomicLong droppedKeys = new AtomicLong();

    private AtomicLong transKeys = new AtomicLong();

    public long incrDropped() {
        return droppedKeys.incrementAndGet();
    }

    public long incrTrans() {
        return transKeys.incrementAndGet();
    }

    public long getDroppedKeys() {
        return droppedKeys.get();
    }

    public long getTransKeys() {
        return transKeys.get();
    }

    @VisibleForTesting
    protected void setDroppedKeys(long droppedKeys) {
        this.droppedKeys.set(droppedKeys);
    }

    @VisibleForTesting
    protected void setTransKeys(long transKeys) {
        this.transKeys.set(transKeys);
    }
}
