package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author lishanglin
 * date 2021/1/18
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultClusterMonitorModifiedNotifier implements ClusterMonitorModifiedNotifier {

    private MonitorManager monitorManager;

    private BeaconMetaService beaconMetaService;

    private MetaCache metaCache;

    private ConsoleCommonConfig config;

    private ConsoleConfig consoleConfig;

    private static final Logger logger = LoggerFactory.getLogger(DefaultClusterMonitorModifiedNotifier.class);
    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    protected static final int MONITOR_NOTIFIER_THREAD_CNT = 5;

    private ExecutorService executors;
    private KeyedOneThreadTaskExecutor<String> keyedExecutor;

    @Autowired
    public DefaultClusterMonitorModifiedNotifier(BeaconMetaService beaconMetaService, MonitorManager monitorManager, MetaCache metaCache, ConsoleCommonConfig config, ConsoleConfig consoleConfig) {
        this.monitorManager = monitorManager;
        this.beaconMetaService = beaconMetaService;
        this.executors = Executors.newFixedThreadPool(MONITOR_NOTIFIER_THREAD_CNT, XpipeThreadFactory.create("ClusterMonitorNotifier"));
        this.keyedExecutor = new KeyedOneThreadTaskExecutor<>(executors);
        this.metaCache = metaCache;
        this.config = config;
        this.consoleConfig = consoleConfig;
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

    private boolean shouldClusterNotify(String clusterName) {
        // DR beacon rule: keep existing supportZones behavior.
        ClusterType clusterType = metaCache.getClusterType(clusterName);
        Set<String> supportZones = config.getBeaconSupportZones();
        if (clusterType.supportSingleActiveDC() && !supportZones.isEmpty()) {
            String activeDc = metaCache.getActiveDc(clusterName);
            if (supportZones.stream().noneMatch(zone -> metaCache.isDcInRegion(activeDc, zone))) {
                logger.info("[notifyClusterUpdate][{}] active dc {} not in {}", clusterName, activeDc, supportZones);
                return false;
            }
        }

        return true;
    }

    @Override
    public void notifyClusterUpdate(final String clusterName, long orgId, String lastModifyTime) {
        try {
            boolean shouldNotifyDr = shouldClusterNotify(clusterName);
            boolean shouldNotifySentinel = shouldSentinelClusterNotify(clusterName, orgId);
            if (!shouldNotifyDr && !shouldNotifySentinel) {
                return;
            }

            keyedExecutor.execute(clusterName, new AbstractCommand<Void>() {
                @Override
                public String getName() {
                    return "NotifyClusterMonitorModified-" + clusterName;
                }

                @Override
                protected void doExecute() {
                    if (shouldNotifyDr) {
                        MonitorService drService = monitorManager.get(orgId, clusterName, BeaconRouteType.DR);
                        if (drService != null) {
                            drService.registerCluster(BeaconSystem.getDefault().getSystemName(), clusterName,
                                    beaconMetaService.buildCurrentBeaconGroups(clusterName),
                                    Collections.singletonMap(BeaconManager.EXTRA_LAST_MODIFY_TIME, lastModifyTime));
                        }
                    }

                    if (shouldNotifySentinel) {
                        MonitorService sentinelService = monitorManager.get(orgId, clusterName, BeaconRouteType.SENTINEL);
                        if (sentinelService != null) {
                            sentinelService.registerCluster(BeaconSystem.getDefault().getSystemName(), clusterName, null,
                                    beaconMetaService.buildBeaconShards(clusterName, currentDc),
                                    Collections.singletonMap(BeaconManager.EXTRA_LAST_MODIFY_TIME, lastModifyTime));
                        }
                    }
                    future().setSuccess();
                }

                @Override
                protected void doReset() {
                    // do nothing
                }
            });
        } catch (Throwable th) {
            logger.info("[notifyClusterUpdate][{}:{}] fail", clusterName, orgId, th);
        }
    }

    @Override
    public void notifyClusterDelete(String clusterName, long orgId) {
        try {
            boolean shouldNotifyDr = shouldClusterNotify(clusterName);
            boolean shouldNotifySentinel = shouldSentinelClusterNotify(clusterName, orgId);
            if (!shouldNotifyDr && !shouldNotifySentinel) {
                return;
            }

            keyedExecutor.execute(clusterName, new AbstractCommand<Void>() {
                @Override
                public String getName() {
                    return "NotifyClusterMonitorDeleted-" + clusterName;
                }

                @Override
                protected void doExecute() {
                    if (shouldNotifyDr) {
                        MonitorService drService = monitorManager.get(orgId, clusterName, BeaconRouteType.DR);
                        if (drService != null) {
                            drService.unregisterCluster(BeaconSystem.getDefault().getSystemName(), clusterName);
                        }
                    }
                    if (shouldNotifySentinel) {
                        MonitorService sentinelService = monitorManager.get(orgId, clusterName, BeaconRouteType.SENTINEL);
                        if (sentinelService != null) {
                            sentinelService.unregisterCluster(BeaconSystem.getDefault().getSystemName(), clusterName);
                        }
                    }
                    future().setSuccess();
                }

                @Override
                protected void doReset() {
                    // do nothing
                }
            });
        } catch (Throwable th) {
            logger.info("[notifyClusterDelete][{}:{}] fail", clusterName, orgId, th);
        }
    }

    private boolean shouldSentinelClusterNotify(String clusterName, long orgId) {
        if (!consoleConfig.supportSentinelBeacon(orgId, clusterName)) {
            return false;
        }

        ClusterMeta currentDcClusterMeta = getCurrentDcClusterMeta(clusterName);
        if (currentDcClusterMeta == null) {
            return false;
        }

        ClusterType clusterType = ClusterType.lookup(currentDcClusterMeta.getType());
        if (currentDcClusterMeta.getAzGroupType() != null && !currentDcClusterMeta.getAzGroupType().isEmpty()) {
            clusterType = ClusterType.lookup(currentDcClusterMeta.getAzGroupType());
        }
        if (!(clusterType == ClusterType.ONE_WAY || clusterType == ClusterType.SINGLE_DC || clusterType == ClusterType.LOCAL_DC)) {
            return false;
        }

        if (clusterType.supportMultiActiveDC()) {
            return true;
        }
        String activeDc = metaCache.getActiveDc(clusterName);
        return currentDc.equalsIgnoreCase(activeDc);
    }

    private ClusterMeta getCurrentDcClusterMeta(String clusterName) {
        if (metaCache.getXpipeMeta() == null || metaCache.getXpipeMeta().getDcs() == null) {
            return null;
        }
        if (!metaCache.getXpipeMeta().getDcs().containsKey(currentDc)) {
            return null;
        }
        return metaCache.getXpipeMeta().getDcs().get(currentDc).getClusters().get(clusterName);
    }


}
