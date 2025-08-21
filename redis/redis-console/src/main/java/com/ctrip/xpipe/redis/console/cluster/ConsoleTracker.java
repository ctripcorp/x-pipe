package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.track.AbstractLeaderTracker;
import org.springframework.stereotype.Component;

@Component
public class ConsoleTracker extends AbstractLeaderTracker implements ConsoleLeaderAware{

    public ConsoleTracker() {
        super("console_leader");
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
