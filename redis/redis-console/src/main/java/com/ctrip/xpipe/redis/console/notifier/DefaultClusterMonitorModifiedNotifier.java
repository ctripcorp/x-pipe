package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
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
 * Dispatches cluster monitor notifications to DR / SENTINEL handlers.
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultClusterMonitorModifiedNotifier implements ClusterMonitorModifiedNotifier {

    private static final Logger logger = LoggerFactory.getLogger(DefaultClusterMonitorModifiedNotifier.class);

    protected static final int MONITOR_NOTIFIER_THREAD_CNT = 5;

    private final DrBeaconClusterMonitorNotifier drNotifier;
    private final SentinelBeaconClusterMonitorNotifier sentinelNotifier;

    private final ExecutorService executors;
    private final KeyedOneThreadTaskExecutor<String> keyedExecutor;

    @Autowired
    public DefaultClusterMonitorModifiedNotifier(DrBeaconClusterMonitorNotifier drNotifier,
                                                 SentinelBeaconClusterMonitorNotifier sentinelNotifier) {
        this.drNotifier = drNotifier;
        this.sentinelNotifier = sentinelNotifier;
        this.executors = Executors.newFixedThreadPool(MONITOR_NOTIFIER_THREAD_CNT,
                XpipeThreadFactory.create("ClusterMonitorNotifier"));
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
    public void notifyClusterUpdate(final String clusterName, long orgId, String lastModifyTime) {
        notifyClusterUpdateInternal(clusterName, null, orgId, lastModifyTime);
    }

    @Override
    public void notifyClusterUpdate(final String clusterName, String dc, long orgId, String lastModifyTime) {
        notifyClusterUpdateInternal(clusterName, dc, orgId, lastModifyTime);
    }

    @Override
    public void notifyClusterDelete(String clusterName, long orgId) {
        notifyClusterDeleteInternal(clusterName, null, orgId);
    }

    @Override
    public void notifyClusterDelete(String clusterName, String dc, long orgId) {
        notifyClusterDeleteInternal(clusterName, dc, orgId);
    }

    private void notifyClusterUpdateInternal(final String clusterName, final String dc, long orgId,
                                             String lastModifyTime) {
        try {
            boolean shouldNotifyDr = drNotifier.needNotify(clusterName, dc, orgId);
            boolean shouldNotifySentinel = sentinelNotifier.needNotify(clusterName, dc, orgId);
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
                        drNotifier.notifyClusterUpdate(clusterName, dc, orgId, lastModifyTime);
                    }
                    if (shouldNotifySentinel) {
                        sentinelNotifier.notifyClusterUpdate(clusterName, dc, orgId, lastModifyTime);
                    }
                    future().setSuccess();
                }

                @Override
                protected void doReset() {
                    // do nothing
                }
            });
        } catch (Throwable th) {
            logger.info("[notifyClusterUpdate][{}:{}][{}] fail", clusterName, orgId, dc, th);
        }
    }

    private void notifyClusterDeleteInternal(String clusterName, String dc, long orgId) {
        try {
            boolean shouldNotifyDr = drNotifier.needNotify(clusterName, dc, orgId);
            boolean shouldNotifySentinel = sentinelNotifier.needNotify(clusterName, dc, orgId);
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
                        drNotifier.notifyClusterDelete(clusterName, dc, orgId);
                    }
                    if (shouldNotifySentinel) {
                        sentinelNotifier.notifyClusterDelete(clusterName, dc, orgId);
                    }
                    future().setSuccess();
                }

                @Override
                protected void doReset() {
                    // do nothing
                }
            });
        } catch (Throwable th) {
            logger.info("[notifyClusterDelete][{}:{}][{}] fail", clusterName, orgId, dc, th);
        }
    }

}
