package com.ctrip.xpipe.redis.console.health.migration.diskless;

import com.ctrip.xpipe.redis.console.health.BaseInstanceResult;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
public class DiskLessInstanceResult extends BaseInstanceResult<RedisInfoAndConf> {

    @Override
    public void success(long rcvNanoTime, RedisInfoAndConf redisInfoAndConf) {
        super.success(rcvNanoTime, redisInfoAndConf);
    }
}
