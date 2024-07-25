package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.track.AbstractLeaderTracker;
import org.springframework.stereotype.Component;

@Component
public class CrossDcLeaderTracker extends AbstractLeaderTracker implements CrossDcLeaderAware {

    public CrossDcLeaderTracker() {
        super( "cross_dc_leader");
    }

    @Override
    public void isCrossDcLeader() {
        doStart();
    }

    @Override
    public void notCrossDcLeader() {
        doStop();
    }

}
