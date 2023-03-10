package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.migration.auto.BeaconSystem;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorServiceManager;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author lishanglin
 * date 2021/1/18
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultClusterMonitorModifiedNotifier implements ClusterMonitorModifiedNotifier {

    private MonitorServiceManager monitorServiceManager;

    private BeaconMetaService beaconMetaService;

    private ConsoleConfig config;

    private static final Logger logger = LoggerFactory.getLogger(DefaultClusterMonitorModifiedNotifier.class);

    protected static final int MONITOR_NOTIFIER_THREAD_CNT = 5;

    private ExecutorService executors;
    private KeyedOneThreadTaskExecutor<String> keyedExecutor;

    @Autowired
    public DefaultClusterMonitorModifiedNotifier(BeaconMetaService beaconMetaService, MonitorServiceManager monitorServiceManager, ConsoleConfig config) {
        this.monitorServiceManager = monitorServiceManager;
        this.beaconMetaService = beaconMetaService;
        this.executors = Executors.newFixedThreadPool(MONITOR_NOTIFIER_THREAD_CNT, XpipeThreadFactory.create("ClusterMonitorNotifier"));
        this.keyedExecutor = new KeyedOneThreadTaskExecutor<>(executors);
        this.config = config;
    }

    @PreDestroy
    public void shutdown() throws Exception {
        if (null != keyedExecutor) {
            keyedExecutor.destroy();
        }
        if (null != executors) {
            executors.shutdown();
        }
    }

    @Override
    public void notifyClusterUpdate(final String clusterName, long orgId) {
        if (config.getMigrationUnsupportedClusters().contains(clusterName.toLowerCase())) {
            logger.info("[notifyClusterUpdate][{}] migration unsupported", clusterName);
            return;
        }

        MonitorService monitorService = monitorServiceManager.getOrCreate(orgId);
        if (null == monitorService) {
            logger.info("[notifyClusterUpdate][{}] no beacon for {}, skip", clusterName, orgId);
            return;
        }

        keyedExecutor.execute(clusterName, new AbstractCommand<Void>() {
            @Override
            public String getName() {
                return "NotifyClusterMonitorModified-" + clusterName;
            }

            @Override
            protected void doExecute() {
                monitorService.registerCluster(BeaconSystem.getDefault().getSystemName(), clusterName, beaconMetaService.buildCurrentBeaconGroups(clusterName));
                future().setSuccess();
            }

            @Override
            protected void doReset() {
                // do nothing
            }
        });
    }

    @Override
    public void notifyClusterDelete(String clusterName, long orgId) {
        MonitorService monitorService = monitorServiceManager.getOrCreate(orgId);
        if (null == monitorService) {
            logger.info("[notifyClusterDelete][{}] no beacon for {}, skip", clusterName, orgId);
            return;
        }

        keyedExecutor.execute(clusterName, new AbstractCommand<Void>() {
            @Override
            public String getName() {
                return "NotifyClusterMonitorDeleted-" + clusterName;
            }

            @Override
            protected void doExecute() {
                monitorService.unregisterCluster(BeaconSystem.getDefault().getSystemName(), clusterName);
                future().setSuccess();
            }

            @Override
            protected void doReset() {
                // do nothing
            }
        });
    }


}
