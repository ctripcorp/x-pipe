package com.ctrip.xpipe.redis.console.healthcheck.nonredis;

import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import org.springframework.beans.factory.annotation.Autowired;


public abstract class AbstractSiteLeaderIntervalCheck extends AbstractIntervalCheck {

    @Autowired(required = false)
    private ConsoleLeaderElector consoleSiteLeader;

    @Override
    protected boolean shouldCheck() {
        if(consoleSiteLeader != null && !consoleSiteLeader.amILeader()) {
            logger.debug("[shouldCheck][not local dc leader, quit]");
            return false;
        }
        return true;
    }
}
