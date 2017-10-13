package com.ctrip.xpipe.redis.console.health.redisconf.diskless;

import com.ctrip.xpipe.redis.console.health.Sample;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
public interface DiskLessCollector {
    void collect(Sample<DiskLessInstanceResult> sample);
}
