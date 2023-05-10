package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig;

import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = {HealthChecker.ENABLED}, matchIfMissing = true)
public class DefaultClientConfigMonitor extends AbstractClientConfigMonitor {

    @Autowired(required = false)
    private ConsoleLeaderElector clusterServer;

    @Override
    protected boolean shouldDoAction() {
        if(clusterServer != null && !clusterServer.amILeader()) {
            logger.debug("[shouldCheck][not site leader, quit]");
            return false;
        }
        return true;
    }
}
