package com.ctrip.xpipe.redis.checker.cluster;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.track.AbstractLeaderTracker;
import org.springframework.stereotype.Component;

@Component
public class AllCheckerLeaderTracker extends AbstractLeaderTracker implements AllCheckerLeaderAware {

    public AllCheckerLeaderTracker() {
        super("all_checker_leader");
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
