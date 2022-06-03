package com.ctrip.xpipe.redis.keeper.applier.sequence.mocks;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.keeper.applier.lwm.ApplierLwmManager;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 16:11
 */
public class TestLwmManager extends AbstractLifecycle implements ApplierLwmManager {

    public long lastSubmitTime = 0;

    public long count = 0;

    @Override
    public synchronized void submit(String gtid) {
        lastSubmitTime = System.currentTimeMillis();
        count ++;
    }
}
