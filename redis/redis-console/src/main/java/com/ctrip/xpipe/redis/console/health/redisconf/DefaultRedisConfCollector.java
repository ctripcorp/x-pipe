package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.health.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 13, 2017
 */
@Component
@Lazy
public class DefaultRedisConfCollector implements RedisConfCollector{

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void collect(Sample<InstanceRedisConfResult> sample) {

        sample.getSamplePlan().getHostPort2SampleResult().forEach((hostPort, result) -> {

            if(result.isSuccess()){
                logger.info("{}: success", hostPort);
            }else {

                logger.info("{}: fail:{}", hostPort, result.getFailReason());
                if(result.getFailReason() instanceof RedisConfFailException){

                    logger.info("{} : conf fail", hostPort);
                    CatEventMonitor.DEFAULT.logAlertEvent(
                        String.format("%s:%s",
                                result.getFailReason().getClass().getSimpleName(), hostPort));
                }
            }

        });

    }
}
