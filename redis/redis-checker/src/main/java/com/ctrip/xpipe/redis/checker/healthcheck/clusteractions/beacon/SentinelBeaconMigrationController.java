package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.CheckInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SentinelBeaconMigrationController implements SentinelBeaconMetaController {

    private static final Logger logger = LoggerFactory.getLogger(SentinelBeaconMigrationController.class);

    private final CheckerDbConfig checkerDbConfig;

    private final PersistenceCache persistenceCache;

    @Autowired
    public SentinelBeaconMigrationController(CheckerDbConfig checkerDbConfig, PersistenceCache persistenceCache) {
        this.checkerDbConfig = checkerDbConfig;
        this.persistenceCache = persistenceCache;
    }

    @Override
    public boolean shouldCheck(ClusterHealthCheckInstance instance) {
        CheckInfo checkInfo = instance.getCheckInfo();
        HealthCheckConfig healthCheckConfig = instance.getHealthCheckConfig();
        if (healthCheckConfig == null) {
            logger.debug("[shouldCheck][{}][skip] health check config unavailable", checkInfo.getClusterId());
            return false;
        }
        if (!healthCheckConfig.supportSentinelBeacon(checkInfo.getClusterOrgId(), checkInfo.getClusterId())) {
            return false;
        }

        String cluster = instance.getCheckInfo().getClusterId();
        if (!checkerDbConfig.shouldSentinelCheck(cluster)) {
            logger.debug("[shouldCheck][{}][skip] sentinel check disabled", cluster);
            return false;
        }

        if (persistenceCache.isClusterOnMigration(cluster)) {
            logger.warn("[{}] in migration, stop check", cluster);
            return false;
        }

        return checkerDbConfig.isSentinelAutoProcess();
    }

}
