package com.ctrip.xpipe.redis.checker.cluster;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.track.AbstractLeaderTracker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GroupCheckerLeaderTracker extends AbstractLeaderTracker implements GroupCheckerLeaderAware{

    @Autowired
    private CheckerConfig config;

    public GroupCheckerLeaderTracker() {
        super("group_checker_leader");
    }

    @Override
    public void isleader() {
        doStart();
    }

    @Override
    public void notLeader() {
        doStop();
    }

    @Override
    protected void addTages(MetricData metricData) {
        metricData.addTag("group", String.valueOf(config.getClustersPartIndex()));
    }
}
