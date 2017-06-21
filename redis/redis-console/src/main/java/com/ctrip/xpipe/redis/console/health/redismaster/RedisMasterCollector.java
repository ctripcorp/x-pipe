package com.ctrip.xpipe.redis.console.health.redismaster;

import com.ctrip.xpipe.redis.console.health.Sample;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 01, 2017
 */
public interface RedisMasterCollector {

    void collect(Sample<InstanceRedisMasterResult> sample);

}
