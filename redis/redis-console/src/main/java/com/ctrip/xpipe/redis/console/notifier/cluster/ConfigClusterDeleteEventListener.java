package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/11/21
 */
@Component
public class ConfigClusterDeleteEventListener implements ClusterDeleteEventListener {

    @Autowired
    private ConfigService configService;

    private static final Logger logger = LoggerFactory.getLogger(ConfigClusterDeleteEventListener.class);

    @Override
    public void update(Object args, Observable observable) {
        EventType type = (EventType) args;
        if(!(observable instanceof ClusterDeleteEvent) || type != EventType.DELETE) {
            logger.info("[update] observable object not ClusterDeleteEvent, skip. observable: {}, args: {}",
                    observable.getClass().getName(),
                    args.getClass().getName());
            return;
        }

        ClusterDeleteEvent clusterDeleteEvent = (ClusterDeleteEvent) observable;
        String clusterName = clusterDeleteEvent.getClusterName();
        try {
            configService.resetClusterWhitelist(clusterName);
        } catch (Throwable th) {
            logger.info("[{}] reset cluster whitelist fail", clusterName, th);
        }
    }
}
