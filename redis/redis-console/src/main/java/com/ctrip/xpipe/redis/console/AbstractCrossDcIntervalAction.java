package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class AbstractCrossDcIntervalAction extends AbstractIntervalAction {

    @Autowired(required = false)
    private CrossDcClusterServer clusterServer;

    @Override
    protected boolean shouldDoAction() {
        if(clusterServer != null && !clusterServer.amILeader()) {
            logger.debug("[shouldReport][not cross dc leader, quit]");
            return false;
        }
        return true;
    }
}
