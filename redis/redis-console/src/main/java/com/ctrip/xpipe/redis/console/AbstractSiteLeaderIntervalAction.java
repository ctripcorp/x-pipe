package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import org.springframework.beans.factory.annotation.Autowired;


public abstract class AbstractSiteLeaderIntervalAction extends AbstractIntervalAction {

    @Autowired(required = false)
    private ConsoleLeaderElector consoleSiteLeader;

    @Override
    protected boolean shouldDoAction() {
        if(consoleSiteLeader != null && !consoleSiteLeader.amILeader()) {
            logger.debug("[shouldCheck][not local dc leader, quit]");
            return false;
        }
        return true;
    }
}
