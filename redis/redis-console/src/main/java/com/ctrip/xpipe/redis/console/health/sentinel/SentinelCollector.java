package com.ctrip.xpipe.redis.console.health.sentinel;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
public interface SentinelCollector {

    void collect(SentinelSample sentinelSample);

}
