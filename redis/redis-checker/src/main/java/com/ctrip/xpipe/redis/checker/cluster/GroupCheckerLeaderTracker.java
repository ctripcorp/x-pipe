package com.ctrip.xpipe.redis.checker.cluster;

import com.ctrip.xpipe.track.AbstractLeaderTracker;
import org.springframework.stereotype.Component;

@Component
public class GroupCheckerLeaderTracker extends AbstractLeaderTracker implements GroupCheckerLeaderAware{

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

}
