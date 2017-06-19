package com.ctrip.xpipe.redis.console.health.sentinel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
@Component
public class DefaultSentinelCollector implements SentinelCollector{

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void collect(SentinelSample sentinelSample) {

        logger.info("[collect]{},{},{}",
                sentinelSample.getSamplePlan().getClusterId(),
                sentinelSample.getSamplePlan().getShardId(),
                sentinelSample.getHellos());

    }
}
