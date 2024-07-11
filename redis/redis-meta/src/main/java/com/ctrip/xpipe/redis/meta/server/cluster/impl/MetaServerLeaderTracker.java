package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.track.AbstractLeaderTracker;
import org.springframework.stereotype.Component;

@Component
public class MetaServerLeaderTracker extends AbstractLeaderTracker implements MetaServerLeaderAware {

    public MetaServerLeaderTracker() {
        super("meta_leader");
    }

    @Override
    public void isleader() {
        doStart();
    }

    @Override
    public void notLeader() {
        doStop();
    }

}
