package com.ctrip.xpipe.redis.console.health.redisconf.version;

import com.ctrip.xpipe.redis.console.health.BaseInstanceResult;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
public class VersionInstanceResult extends BaseInstanceResult<String> {

    @Override
    public void success(long rcvNanoTime, String version) {
        super.success(rcvNanoTime, version);
    }

}
