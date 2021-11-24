package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler.HealthEventHandler;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
@Component
@Lazy
public class OuterClientServiceProcessor implements HealthEventProcessor {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private List<HealthEventHandler> eventHandlers;

    @Override
    public void onEvent(AbstractInstanceEvent instanceEvent) {

        RedisInstanceInfo info = instanceEvent.getInstance().getCheckInfo();
        if(!instanceInBackupDc(info.getHostPort())) {
            logger.info("[onEvent][instance not in backupDc] {}, {}", instanceEvent.getClass().getSimpleName(), info);
            return;
        }
        for(HealthEventHandler handler : eventHandlers) {
            try {
                handler.handle(instanceEvent);
            } catch (Exception e) {
                logger.error("[tryHandle] instance event: {}, instance: {}", instanceEvent.getClass().getSimpleName(),
                        instanceEvent.getInstance().getEndpoint(), e);
            }
        }
    }

    private boolean instanceInBackupDc(HostPort hostPort) {
        return metaCache.inBackupDc(hostPort);
    }

    @VisibleForTesting
    public OuterClientServiceProcessor setEventHandlers(List<HealthEventHandler> eventHandlers) {
        this.eventHandlers = eventHandlers;
        return this;
    }
}