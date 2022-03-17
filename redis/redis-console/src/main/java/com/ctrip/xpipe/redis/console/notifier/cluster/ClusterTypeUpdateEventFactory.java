package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

@Component
public class ClusterTypeUpdateEventFactory extends AbstractClusterEventFactory {

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Autowired
    private List<ClusterTypeUpdateEventListener> clusterEventListeners;

    @Override
    public ClusterEvent createClusterEvent(String clusterName, ClusterTbl clusterTbl) {
        ClusterTypeUpdateEvent clusterTypeUpdateEvent = new ClusterTypeUpdateEvent(clusterName, clusterTbl.getClusterOrgId(), executors);
        clusterTypeUpdateEvent.setClusterType(ClusterType.lookup(clusterTbl.getClusterType()));

        clusterEventListeners
                .forEach(clusterTypeUpdateEventListener -> clusterTypeUpdateEvent.addObserver(clusterTypeUpdateEventListener));

        return clusterTypeUpdateEvent;
    }
}
