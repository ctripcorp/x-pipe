package com.ctrip.xpipe.redis.console.health.redisconf.diskless;

import com.ctrip.xpipe.redis.console.health.BaseInstanceResult;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
public class DiskLessInstanceResult extends BaseInstanceResult<List<String>> {

    @Override
    public void success(long rcvNanoTime, List<String> message) {
        super.success(rcvNanoTime, message);
    }
}
