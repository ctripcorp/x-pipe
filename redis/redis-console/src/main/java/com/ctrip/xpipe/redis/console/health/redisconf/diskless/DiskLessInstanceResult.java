package com.ctrip.xpipe.redis.console.health.redisconf.diskless;

import com.ctrip.xpipe.redis.console.health.BaseInstanceResult;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
public class DiskLessInstanceResult extends BaseInstanceResult<Boolean> {

    @Override
    public void success(long rcvNanoTime, Boolean isDisklessSync) {
        super.success(rcvNanoTime, isDisklessSync);
    }
}
