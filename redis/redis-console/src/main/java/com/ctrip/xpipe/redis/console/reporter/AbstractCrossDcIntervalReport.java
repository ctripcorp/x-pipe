package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class AbstractCrossDcIntervalReport extends AbstractIntervalReport {

    @Autowired(required = false)
    private CrossDcClusterServer clusterServer;

    @Override
    protected boolean shouldReport() {
        if(clusterServer != null && !clusterServer.amILeader()) {
            logger.debug("[shouldReport][not cross dc leader, quit]");
            return false;
        }
        return true;
    }
}
