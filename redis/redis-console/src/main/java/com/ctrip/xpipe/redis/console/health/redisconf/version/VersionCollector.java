package com.ctrip.xpipe.redis.console.health.redisconf.version;

import com.ctrip.xpipe.redis.console.health.Sample;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
public interface VersionCollector {
    void collect(Sample<VersionInstanceResult> result);
}
