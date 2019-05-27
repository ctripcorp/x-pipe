package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.redis.console.healthcheck.HealthChecker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = {HealthChecker.ENABLED}, matchIfMissing = true)
public class DefaultClientConfigMonitor extends AbstractClientConfigMonitor {

    @Autowired(required = false)
    private CrossDcClusterServer clusterServer;

    @Override
    protected boolean shouldCheck() {
        if(clusterServer != null && !clusterServer.amILeader()) {
            logger.debug("[shouldCheck][not cross dc leader, quit]");
            return false;
        }
        return true;
    }
}
