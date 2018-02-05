package com.ctrip.xpipe.redis.console.health.redisconf.backlog;

import com.ctrip.xpipe.redis.console.health.Sample;

/**
 * @author chen.zhu
 * <p>
 * Feb 05, 2018
 */
public interface BacklogActiveCollector {

    void collect(Sample<InstanceInfoReplicationResult> sample);
}
