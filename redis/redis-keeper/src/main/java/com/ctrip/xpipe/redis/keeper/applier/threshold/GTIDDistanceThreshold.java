package com.ctrip.xpipe.redis.keeper.applier.threshold;

import com.ctrip.xpipe.utils.OffsetNotifier;

/**
 * @author Slight
 * <p>
 * Jan 17, 2023 19:40
 */
public class GTIDDistanceThreshold extends OffsetNotifier {

    public final long distance;

    public boolean firstCalled = true;

    long lastSubmit = 0;

    public GTIDDistanceThreshold(long distance) {
        super(0);
        this.distance = distance;
    }

    public void firstCalled(long lwmSum) {
        if (firstCalled) {
            firstCalled = false;
            submit(lwmSum - 1);
        }
    }

    public void tryPass(long lwmSum) throws InterruptedException {
        firstCalled(lwmSum);
        super.await(lwmSum);
    }

    public boolean tryPass(long lwmSum, long timeMilli) throws InterruptedException {
        firstCalled(lwmSum);
        return super.await(lwmSum, timeMilli);
    }

    public synchronized boolean submit(long lwmSum) {
        if (lwmSum >= lastSubmit) {
            lastSubmit = lwmSum;
            super.offsetIncreased(lwmSum + distance);
            return true;
        }
        return false;
    }
}
