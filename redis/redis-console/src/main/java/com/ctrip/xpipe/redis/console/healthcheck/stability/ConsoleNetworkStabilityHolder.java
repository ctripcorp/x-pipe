package com.ctrip.xpipe.redis.console.healthcheck.stability;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class ConsoleNetworkStabilityHolder implements ConsoleLeaderAware, NetworkStabilityHolder {

    @Autowired
    private ConsoleServiceManager consoleServiceManager;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleConfig config;

    @Autowired
    private NetworkStabilityInspector inspector;

    private DynamicDelayPeriodTask task;

    protected AtomicBoolean taskTrigger = new AtomicBoolean(false);

    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();

    private AtomicBoolean dcIsolated = new AtomicBoolean(false);

    protected ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1,
            XpipeThreadFactory.create("ConsoleNetworkStabilityHolder"));


    @Override
    public void isleader() {
        if (taskTrigger.compareAndSet(false, true)) {
            try {
                stop();
                start();
            } catch (Exception e) {

            }
        }
    }

    @Override
    public void notLeader() {
        if (taskTrigger.compareAndSet(true, false)) {
            try {
                stop();
            } catch (Exception e) {

            }
        }
    }

    private void stop() throws Exception {
        if (task != null) {
            task.stop();
            task = null;
        }
    }

    private void start() throws Exception {
        task = new DynamicDelayPeriodTask("ConsoleNetworkStabilityHolder", new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                check();
            }
        }, () -> config.getRedisReplicationHealthCheckInterval(), scheduled);
        task.start();
    }

    private void check() {

        if (!config.checkDcNetwork()) {
            dcIsolated.set(false);
            return;
        }

        List<String> dcsInCurrentRegion = metaCache.regionDcs(CURRENT_DC);
        dcsInCurrentRegion.remove(CURRENT_DC);

        if (dcsInCurrentRegion.size() < config.getQuorum()) {
            // CFT e.g.
            String delegateDc = config.delegateDc();
            if (Strings.isNullOrEmpty(delegateDc)) {
                dcIsolated.set(false);
                return;
            }

            Boolean delegateDcCheckResult = consoleServiceManager.getDcIsolatedCheckResult(delegateDc);
            if (delegateDcCheckResult == null)
                return;

            dcIsolated.set(delegateDcCheckResult);
        } else {
            AtomicBoolean mayIsolated = new AtomicBoolean(inspector.isolated());

            if (!mayIsolated.get()) {
                dcIsolated.set(false);
            } else {
                Map<String, Boolean> allConsoleResults = consoleServiceManager.getAllDcIsolatedCheckResult();

                allConsoleResults.forEach((s, v) -> {
                    if(!s.equalsIgnoreCase(CURRENT_DC)){
                        mayIsolated.set(mayIsolated.get() & v);
                    }
                });

                if (mayIsolated.get()) {
                    dcIsolated.set(true);
                } else {
                    dcIsolated.set(false);
                }
            }
        }

    }

    @Override
    public boolean isolated() {
        return dcIsolated.get();
    }

}
