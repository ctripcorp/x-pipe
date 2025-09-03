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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private Logger logger = LoggerFactory.getLogger(ConsoleNetworkStabilityHolder.class);

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
                logger.error("[isleader]", e);
            }
        }
    }

    @Override
    public void notLeader() {
        if (taskTrigger.compareAndSet(true, false)) {
            try {
                stop();
            } catch (Exception e) {
                logger.error("[notleader]", e);
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
        }, () -> config.getCheckIsolateInterval(), scheduled);
        task.start();
    }

    void check() {
        Boolean configIsolated = config.getDcIsolated();

        if (configIsolated != null) {
            dcIsolated.set(configIsolated);
            return;
        }

        List<String> dcsInCurrentRegion = metaCache.regionDcs(CURRENT_DC);
        dcsInCurrentRegion.remove(CURRENT_DC);

        if (dcsInCurrentRegion.size() < config.getQuorum()) {
            getResultFromDelegateDcConsole();
        } else {
            aggregateResultsFromDcServers();
        }
    }

    void getResultFromDelegateDcConsole() {
        // CFT e.g.
        String delegateDc = config.delegateDc();
        if (Strings.isNullOrEmpty(delegateDc)) {
            dcIsolated.set(false);
            return;
        }

        Boolean delegateDcCheckResult = consoleServiceManager.getDcIsolatedCheckResult(delegateDc);
        if (delegateDcCheckResult == null) {
            logger.warn("[check]get check result from dc {}, but not found", delegateDc);
            return;
        }

        dcIsolated.set(delegateDcCheckResult);
    }

    void aggregateResultsFromDcServers() {
        AtomicBoolean mayIsolated = new AtomicBoolean(inspector.isolated());

        if (!mayIsolated.get()) {
            dcIsolated.set(false);
        } else {
            try {
                Map<String, Boolean> allConsoleResults = consoleServiceManager.getAllDcIsolatedCheckResult();
                logger.info("[aggregateResultsFromAllServers]results: {}", allConsoleResults);
                allConsoleResults.forEach((s, v) -> {
                    mayIsolated.set(mayIsolated.get() & v);
                });

                dcIsolated.set(mayIsolated.get());
            } catch (Throwable throwable) {
                logger.error("[aggregateResultsFromAllServers]keep current result:{}", dcIsolated.get(), throwable);
            }
        }
    }

    @Override
    public boolean isolated() {
        return dcIsolated.get();
    }

}
