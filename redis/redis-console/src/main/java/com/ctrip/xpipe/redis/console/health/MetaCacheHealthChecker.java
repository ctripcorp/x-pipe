package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Nov 20, 2017
 */
@Component
public class MetaCacheHealthChecker {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    MetaCache metaCache;

    @Resource(name= ConsoleContextConfig.SCHEDULED_EXECUTOR)
    ScheduledExecutorService scheduled;

    @PostConstruct
    public void postConstruct() {
        scheduled.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    logger.info("[postConstruct] \n {}", metaCache.getXpipeMeta().toString());
                } catch (Exception e) {
                    logger.error("[postConstruct]", e);
                }
            }
        }, 2, 1, TimeUnit.SECONDS);
    }
}
