package com.ctrip.xpipe.redis.console.healthcheck.nonredis;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorServiceManager;
import org.springframework.beans.factory.annotation.Autowired;


/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
public abstract class AbstractCrossDcIntervalCheck extends AbstractIntervalCheck {

    @Autowired(required = false)
    private CrossDcClusterServer clusterServer;

    @Autowired
    private MonitorServiceManager monitorServiceManager;

    @Override
    protected boolean shouldCheck() {
        if(clusterServer != null && !clusterServer.amILeader()) {
            logger.debug("[shouldCheck][not cross dc leader, quit]");
            return false;
        }
        return true;
    }
}
