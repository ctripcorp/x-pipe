package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 * @author lishanglin
 * date 2022/7/22
 */
public class InstanceStatusAdjustCommand extends AbstractCommand<Void> {

    private ClusterShardHostPort instance;

    private boolean state;

    private long deadlineTimeMilli;

    private long noModifySeconds;

    private MetaCache metaCache;

    private OuterClientService outerClientService;

    private AlertManager alertManager;

    private static final Logger logger = LoggerFactory.getLogger(InstanceStatusAdjustCommand.class);

    public InstanceStatusAdjustCommand(ClusterShardHostPort instance, boolean state, long deadlineTimeMilli, long noModifySeconds,
                                       MetaCache metaCache, OuterClientService outerClientService, AlertManager alertManager) {
        this.instance = instance;
        this.state = state;
        this.deadlineTimeMilli = deadlineTimeMilli;
        this.noModifySeconds = noModifySeconds;
        this.metaCache = metaCache;
        this.outerClientService = outerClientService;
        this.alertManager = alertManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        if (System.currentTimeMillis() > deadlineTimeMilli) {
            logger.info("[compensate][skip] timeout {}, instance {}", deadlineTimeMilli, instance);
            future().setFailure(new TimeoutException(instance.toString()));
            return;
        }
        if (!metaCache.inBackupDc(instance.getHostPort())) {
            logger.info("[compensate][skip][active dc instance] {}", instance);
            future().setFailure(new IllegalArgumentException("instance not in backup dc"));
            return;
        }
        if (state == outerClientService.isInstanceUp(instance)) {
            logger.info("[compensate][skip][already consistent] {}", instance);
            future().setSuccess();
            return;
        }

        logger.info("[compensate][{}] {}", state ? "up" : "down", instance);
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

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return "InstanceStatusAdjustCommand:" + (state ? "Up" : "Down") + ":" + instance;
    }
}
