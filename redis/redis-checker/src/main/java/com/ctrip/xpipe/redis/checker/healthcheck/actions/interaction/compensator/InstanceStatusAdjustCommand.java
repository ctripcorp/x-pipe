package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author lishanglin
 * date 2022/7/22
 */
public class InstanceStatusAdjustCommand extends AbstractCommand<Void> {

    private ClusterShardHostPort instance;

    private InstanceHealthStatusCollector collector;

    private OuterClientService outerClientService;

    private boolean state;

    private long deadlineTimeMilli;

    private MetaCache metaCache;

    private CheckerConfig config;

    private AlertManager alertManager;

    private static final Logger logger = LoggerFactory.getLogger(InstanceStatusAdjustCommand.class);

    public InstanceStatusAdjustCommand(ClusterShardHostPort instance, InstanceHealthStatusCollector collector,
                                       OuterClientService outerClientService, boolean state, long deadlineTimeMilli,
                                       CheckerConfig config, MetaCache metaCache, AlertManager alertManager) {
        this.instance = instance;
        this.collector = collector;
        this.outerClientService = outerClientService;
        this.state = state;
        this.config = config;
        this.deadlineTimeMilli = deadlineTimeMilli;
        this.metaCache = metaCache;
        this.alertManager = alertManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        if (checkTimeout()) return;
        if (config.isConsoleSiteUnstable()) {
            logger.info("[compensate][skip][unstable] {}", instance);
            future().setSuccess();
            return;
        }
        if (!metaCache.inBackupDc(instance.getHostPort())) {
            logger.info("[compensate][skip][active dc instance] {}", instance);
            future().setFailure(new IllegalArgumentException("instance not in backup dc"));
            return;
        }
        if (state == outerClientService.isInstanceUp(instance)) {
            logger.info("[compensate][skip][already consistent] {} {}", state, instance);
            future().setSuccess();
            return;
        }
        Boolean xpipeHealthState =
                collector.collectXPipeInstanceHealth(instance.getHostPort()).aggregate(instance.getHostPort(), config.getQuorum());
        if (null == xpipeHealthState || !xpipeHealthState.equals(state)) {
            logger.info("[compensate][skip][xpipe state change] {}->{} {}", state, xpipeHealthState, instance);
            future().setSuccess();
            return;
        }

        if (checkTimeout()) return;
        logger.info("[compensate][{}] {}", state ? "up" : "down", instance);
        long noModifySeconds = TimeUnit.MILLISECONDS.toSeconds(config.getHealthMarkCompensateIntervalMill());
        if (state) {
            alertManager.alert(instance.getClusterName(), instance.getShardName(), instance.getHostPort(),
                    ALERT_TYPE.COMPENSATE_MARK_INSTANCE_UP, "Mark instance up");
            outerClientService.markInstanceUpIfNoModifyFor(instance, noModifySeconds);
        } else {
            alertManager.alert(instance.getClusterName(), instance.getShardName(), instance.getHostPort(),
                    ALERT_TYPE.COMPENSATE_MARK_INSTANCE_DOWN, "Mark instance down");
            outerClientService.markInstanceDownIfNoModifyFor(instance, noModifySeconds);
        }

        future().setSuccess();
    }

    private boolean checkTimeout() {
        if (System.currentTimeMillis() > deadlineTimeMilli) {
            logger.info("[compensate][skip] timeout {}, instance {}", deadlineTimeMilli, instance);
            future().setFailure(new TimeoutException(instance.toString()));
            return true;
        }

        return false;
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return "InstanceStatusAdjustCommand:" + (state ? "Up" : "Down") + ":" + instance;
    }
}
