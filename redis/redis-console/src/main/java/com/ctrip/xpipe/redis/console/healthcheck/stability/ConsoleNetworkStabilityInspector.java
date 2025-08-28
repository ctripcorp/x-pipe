package com.ctrip.xpipe.redis.console.healthcheck.stability;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ConsoleNetworkStabilityInspector extends AbstractLifecycle implements TopElement, NetworkStabilityInspector {

    private MetaCache metaCache;

    private ConsoleConfig config;

    private ConsoleServiceManager consoleServiceManager;

    private AtomicBoolean dcIsolated = new AtomicBoolean(false);

    private AtomicInteger continuousMismatchTimes = new AtomicInteger();

    private DynamicDelayPeriodTask task;

    private ScheduledExecutorService scheduled;

    private static final String DC_ISOLATED_TYPE = "isolated";
    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();
    static final int CONNECT_TIMEOUT = 1200;
    static final int COMMAND_TIMEOUT = 1500;


    public ConsoleNetworkStabilityInspector(MetaCache metaCache, ConsoleConfig consoleConfig, ConsoleServiceManager consoleServiceManager) {
        this.metaCache = metaCache;
        this.config = consoleConfig;
        this.consoleServiceManager = consoleServiceManager;
    }


    @Override
    public boolean isolated() {
        return dcIsolated.get();
    }

    @VisibleForTesting
    protected void inspect() {
        logger.debug("[inspect] begin");
        checkDcIsolated();
    }

    void checkDcIsolated() {
        try {
            if (!config.checkDcNetwork()) {
                dcIsolated.set(false);
                return;
            }

            List<String> dcsInCurrentRegion = metaCache.regionDcs(CURRENT_DC);
            dcsInCurrentRegion.remove(CURRENT_DC);

            if (dcsInCurrentRegion.isEmpty()) {
                logger.warn("[checkDcIsolated]No other dcs found in current region, current dc: {}", CURRENT_DC);
                return;
            }

            boolean mayIsolated;
            ParallelCommandChain chain = new ParallelCommandChain();
            Map<String, Boolean> allDcResults = new ConcurrentHashMap<>();
            for (String dcId : dcsInCurrentRegion) {
                chain.add(new AbstractCommand<Boolean>() {
                    @Override
                    protected void doExecute() throws Throwable {
                        consoleServiceManager.connectDc(dcId, CONNECT_TIMEOUT).addListener(connectFuture -> {
                            if (connectFuture.isSuccess()) {
                                allDcResults.put(dcId, connectFuture.get());
                                future().setSuccess(true);
                            } else {
                                logger.error("[checkDcIsolated]{}", dcId, connectFuture.cause());
                                future().setSuccess(false);
                            }
                        });
                    }

                    @Override
                    protected void doReset() {

                    }

                    @Override
                    public String getName() {
                        return "connectConsole";
                    }
                });
            }

            try {
                chain.execute().get(COMMAND_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (Throwable th) {
                logger.warn("[checkDcIsolated]command timeout, expected dcs:{}, actual results:{}", dcsInCurrentRegion, allDcResults);
            }

            boolean dcConnected = connected(allDcResults);

            if (dcConnected) {
                //at least 1 dc connected
                mayIsolated = false;
            } else if (dcsInCurrentRegion.size() == allDcResults.size()) {
                //all dcs connect failed
                mayIsolated = true;
            } else {
                //results not enough
                logger.warn("[checkDcIsolated]missing result, expected dcs:{}, actual dcs:{}", dcsInCurrentRegion, allDcResults);
                return;
            }

            incrMismatchIfNeeded(mayIsolated);
            toggleIsolatedIfNeeded();
            EventMonitor.DEFAULT.logEvent(DC_ISOLATED_TYPE, dcIsolated.get() ? "isolated" : "unisolated");
        } catch (Throwable th) {
            logger.error("[checkDcIsolated]", th);
        }
    }


    boolean connected(Map<String, Boolean> allDcResults) {
        for (Map.Entry<String, Boolean> entry : allDcResults.entrySet()) {
            if (entry.getValue())
                return true;
        }
        return false;
    }

    private void incrMismatchIfNeeded(boolean mayIsolated) {
        if (mayIsolated != dcIsolated.get()) {
            int after = continuousMismatchTimes.incrementAndGet();
            logger.info("[incrMismatchIfNeeded] may {}:{}", mayIsolated ? "isolated":"unisolated", after);
        } else if (continuousMismatchTimes.get() > 0) {
            logger.info("[incrMismatchIfNeeded][reset]");
            this.continuousMismatchTimes.set(0);
        }
    }

    private void toggleIsolatedIfNeeded() {
        if (dcIsolated.get() && continuousMismatchTimes.get() >= config.getIsolateRecoverAfterRounds()) {
            logger.info("[toggleIsolatedIfNeeded] become unisolated");
            setDcIsolated(false);
        } else if (!dcIsolated.get() && continuousMismatchTimes.get() >= config.getIsolateAfterRounds()) {
            logger.info("[toggleIsolatedIfNeeded] become isolated");
            setDcIsolated(true);
        }
    }

    protected void setDcIsolated(boolean dcIsolated) {
        this.dcIsolated.set(dcIsolated);
        this.continuousMismatchTimes.set(0);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("NetworkStabilityInspector"));
        this.task = new DynamicDelayPeriodTask("NetworkStabilityInspector", new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                inspect();
            }
        }, config::getRedisReplicationHealthCheckInterval, scheduled);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        this.task.start();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        this.task.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        this.scheduled.shutdown();
        this.scheduled = null;
        this.task = null;
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }


    @Autowired
    public void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

    @Autowired
    public void setConfig(ConsoleConfig config) {
        this.config = config;
    }
}
