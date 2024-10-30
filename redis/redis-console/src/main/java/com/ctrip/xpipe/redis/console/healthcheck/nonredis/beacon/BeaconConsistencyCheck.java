package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class BeaconConsistencyCheck extends AbstractCrossDcIntervalAction {

    @Autowired
    private MonitorManager monitorManager;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleCommonConfig config;

    @Override
    protected void doAction() {
        Map<Long, List<MonitorService>> services = monitorManager.getAllServices();

        if (services.isEmpty()) {
            logger.debug("[doCheck] no beacon service, skip");
        }

        Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg = monitorManager.clustersByBeaconSystemOrg();

        new BeaconConsistencyCheckJob(clustersByBeaconSystemOrg, services, metaCache, config)
                .execute()
                .addListener(commandFuture -> {
                    if (!commandFuture.isSuccess()) {
                        logger.error("BeaconConsistencyCheck,fail");
                    }
                });
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.emptyList();
    }
}
