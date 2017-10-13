package com.ctrip.xpipe.redis.console.health.redisconf.rewrite;

import com.ctrip.xpipe.redis.console.health.Sample;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 13, 2017
 */
public interface RedisConfCollector {

    void collect(Sample<InstanceRedisConfResult> sample);

}
