package com.ctrip.xpipe.redis.keeper.health;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.health.job.DiskIOStatCheckJob;
import com.ctrip.xpipe.redis.keeper.health.job.DiskReadWriteCheckJob;
import com.ctrip.xpipe.redis.keeper.health.job.DiskSpaceUsageCheckJob;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lishanglin
 * date 2023/11/9
 */
@Component
public class DiskHealthChecker extends AbstractLifecycleObservable implements TopElement, Observable {

    KeeperContainerConfig keeperContainerConfig;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask checkTask;

    private AtomicReference<HealthState> state;

    private AtomicInteger rounds;

    private AtomicReference<KeeperDiskInfo> result;

    private static final int checkTimeoutSeconds = Integer.parseInt(System.getProperty("keeper.disk.check.timeout.sec", "10"));

    public DiskHealthChecker(KeeperContainerConfig keeperContainerConfig) {
        this.keeperContainerConfig = keeperContainerConfig;
        this.result = new AtomicReference<>(new KeeperDiskInfo());
        this.state = new AtomicReference<>(HealthState.HEALTHY);
        this.rounds = new AtomicInteger(0);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        String name = getClass().getSimpleName();
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(name));
        this.checkTask = new DynamicDelayPeriodTask(name, this::check, keeperContainerConfig::diskCheckInterval, scheduled);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        this.checkTask.start();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        this.checkTask.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        this.scheduled.shutdown();
    }

    public void check() {
        try {
            String storePath = keeperContainerConfig.getReplicationStoreDir();
            logger.debug("[check][start] {}", storePath);
            KeeperDiskInfo diskInfo = new KeeperDiskInfo();
            DiskReadWriteCheckJob diskReadWriteCheckJob = new DiskReadWriteCheckJob(storePath);
            DiskSpaceUsageCheckJob diskSpaceUsageCheckJob = new DiskSpaceUsageCheckJob(storePath);

            diskInfo.available = diskReadWriteCheckJob.execute().get(checkTimeoutSeconds, TimeUnit.SECONDS);
            diskInfo.spaceUsageInfo = diskSpaceUsageCheckJob.execute().get(checkTimeoutSeconds, TimeUnit.SECONDS);
            if (null != diskInfo.spaceUsageInfo && !StringUtil.isEmpty(diskInfo.spaceUsageInfo.source)) {
                DiskIOStatCheckJob diskIOStatCheckJob = new DiskIOStatCheckJob(diskInfo.spaceUsageInfo.getDevice());
                diskInfo.ioStatInfo = diskIOStatCheckJob.execute().get(checkTimeoutSeconds, TimeUnit.SECONDS);
            }

            logger.debug("[check][end] {}", diskInfo);
            setResult(diskInfo);
        } catch (Throwable th) {
            logger.info("[check] fail", th);
        }
    }

    protected void setResult(KeeperDiskInfo diskInfo) {
        this.result.set(diskInfo);
        if (diskInfo.available) {
            state.set(HealthState.HEALTHY);
        } else {
            if (state.get() == HealthState.HEALTHY) {
                rounds.set(1);
                if (rounds.get() >= keeperContainerConfig.checkRoundBeforeMarkDown()) {
                    state.set(HealthState.DOWN);
                } else {
                    state.set(HealthState.SICK);
                }
            } else if (state.get() == HealthState.SICK) {
                if (rounds.incrementAndGet() >= keeperContainerConfig.checkRoundBeforeMarkDown()) {
                    state.set(HealthState.DOWN);
                }
            } else {
                // do nothing
            }
        }

        notifyObservers(this.state.get());
    }

    public KeeperDiskInfo getResult() {
        return result.get();
    }

    public HealthState getState() {
        return state.get();
    }

}
