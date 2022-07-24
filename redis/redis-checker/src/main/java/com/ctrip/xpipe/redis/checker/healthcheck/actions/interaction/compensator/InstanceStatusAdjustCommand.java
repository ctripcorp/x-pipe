package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
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

    private OuterClientService outerClientService;

    private AlertManager alertManager;

    private static final Logger logger = LoggerFactory.getLogger(InstanceStatusAdjustCommand.class);

    public InstanceStatusAdjustCommand(ClusterShardHostPort instance, boolean state, long deadlineTimeMilli,
                                       OuterClientService outerClientService, AlertManager alertManager) {
        this.instance = instance;
        this.state = state;
        this.deadlineTimeMilli = deadlineTimeMilli;
        this.outerClientService = outerClientService;
        this.alertManager = alertManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        if (System.currentTimeMillis() > deadlineTimeMilli) {
            future().setFailure(new TimeoutException(instance.toString()));
            logger.debug("[doExecute][skip] timeout {}, instance {}", deadlineTimeMilli, instance);
            return;
        }

        if (state) {
            alertManager.alert(instance.getClusterName(), instance.getShardName(), instance.getHostPort(),
                    ALERT_TYPE.COMPENSATE_MARK_INSTANCE_UP, "Mark instance up");
            outerClientService.markInstanceUp(instance);
        } else {
            alertManager.alert(instance.getClusterName(), instance.getShardName(), instance.getHostPort(),
                    ALERT_TYPE.COMPENSATE_MARK_INSTANCE_DOWN, "Mark instance down");
            outerClientService.markInstanceDown(instance);
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
