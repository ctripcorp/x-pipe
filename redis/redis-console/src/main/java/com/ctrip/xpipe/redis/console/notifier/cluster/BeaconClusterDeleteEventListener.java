package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.console.notifier.ClusterMonitorModifiedNotifier;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/1/10
 */
@Component
public class BeaconClusterDeleteEventListener implements ClusterDeleteEventListener {

    private static Logger logger = LoggerFactory.getLogger(BeaconClusterDeleteEventListener.class);

    private ClusterMonitorModifiedNotifier notifier;

    @Autowired
    public BeaconClusterDeleteEventListener(ClusterMonitorModifiedNotifier notifier) {
        this.notifier = notifier;
    }

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
        long clusterOrgId = clusterDeleteEvent.getOrgId();
        if (clusterDeleteEvent.getClusterType().supportMigration())
            notifier.notifyClusterDelete(clusterName, clusterOrgId);
    }

}
