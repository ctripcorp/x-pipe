package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.console.beacon.BeaconService;
import com.ctrip.xpipe.redis.console.beacon.BeaconServiceManager;
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

    private BeaconServiceManager beaconServiceManager;

    private BeaconMetaService beaconMetaService;

    private static final Logger logger = LoggerFactory.getLogger(DefaultClusterMonitorModifiedNotifier.class);

    protected static final int MONITOR_NOTIFIER_THREAD_CNT = 5;

    private ExecutorService executors;
    private KeyedOneThreadTaskExecutor<String> keyedExecutor;

    @Autowired
    public DefaultClusterMonitorModifiedNotifier(BeaconMetaService beaconMetaService, BeaconServiceManager beaconServiceManager) {
        this.beaconServiceManager = beaconServiceManager;
        this.beaconMetaService = beaconMetaService;
        this.executors = Executors.newFixedThreadPool(MONITOR_NOTIFIER_THREAD_CNT, XpipeThreadFactory.create("ClusterMonitorNotifier"));
        this.keyedExecutor = new KeyedOneThreadTaskExecutor<>(executors);
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
        BeaconService beaconService = beaconServiceManager.getOrCreate(orgId);
        if (null == beaconService) {
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
                beaconService.registerCluster(clusterName, beaconMetaService.buildCurrentBeaconGroups(clusterName));
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
        BeaconService beaconService = beaconServiceManager.getOrCreate(orgId);
        if (null == beaconService) {
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
                beaconService.unregisterCluster(clusterName);
                future().setSuccess();
            }

            @Override
            protected void doReset() {
                // do nothing
            }
        });
    }


}
