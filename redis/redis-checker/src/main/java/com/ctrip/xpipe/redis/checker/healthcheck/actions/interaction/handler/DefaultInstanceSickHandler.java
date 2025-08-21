package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.utils.VisibleForTesting;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
@Component
public class DefaultInstanceSickHandler extends AbstractHealthEventHandler<InstanceSick> implements InstanceSickHandler {

    @Autowired
    private CheckerConfig checkerConfig;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    protected static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Override
    protected void doHandle(InstanceSick instanceSick) {
        if (!shouldMarkdownDcClusterSickInstances(instanceSick))
            return;

        tryMarkDown(instanceSick);
    }

    boolean shouldMarkdownDcClusterSickInstances(InstanceSick instanceSick) {
        RedisInstanceInfo info = instanceSick.getInstance().getCheckInfo();
        if (info.isCrossRegion()) {
            logger.info("[markdown][{} is cross region, do not call client service ]{}", info, instanceSick);
            return false;
        }
        if (instanceSick.getInstance().getHealthCheckConfig().getDelayConfig(info.getClusterId(), currentDcId, info.getDcId()).getClusterLevelHealthyDelayMilli() < 0) {
            logger.info("[markdown][cluster {} dcs {}->{} distance is -1, do not call client service ]{}", info.getClusterId(), currentDcId, info.getDcId(), instanceSick);
            return false;
        }
        return true;
    }

    @Override
    protected void doMarkDown(final AbstractInstanceEvent event) {
        doRealMarkDown(event);
    }

    @VisibleForTesting
    public DefaultInstanceSickHandler setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }
}
