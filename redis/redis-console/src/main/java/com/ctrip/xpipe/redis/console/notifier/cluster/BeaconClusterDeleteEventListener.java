package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.api.observer.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/1/10
 */
@Component
public class BeaconClusterDeleteEventListener implements ClusterDeleteEventListener {

    private static Logger logger = LoggerFactory.getLogger(BeaconClusterDeleteEventListener.class);

    @Override
    public void update(Object args, Observable observable) {
        // TODO: unregister cluster from beacon
    }

}
