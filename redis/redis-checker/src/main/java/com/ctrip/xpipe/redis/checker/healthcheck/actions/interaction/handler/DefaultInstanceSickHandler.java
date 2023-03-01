package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private static final List<HEALTH_STATE> satisfiedStates = Lists.newArrayList(HEALTH_STATE.DOWN, HEALTH_STATE.SICK, HEALTH_STATE.UNHEALTHY);

    @Override
    protected List<HEALTH_STATE> getSatisfiedStates() {
        return satisfiedStates;
    }

    protected DcClusterDelayMarkDown getDelayMarkDownIfConfiged(AbstractInstanceEvent event) {
        RedisInstanceInfo info = event.getInstance().getCheckInfo();
        Set<DcClusterDelayMarkDown> dcClusters = checkerConfig.getDelayedMarkDownDcClusters();
        if(dcClusters != null) {
            for(DcClusterDelayMarkDown config : dcClusters) {
                if(config.matches(info.getDcId(), info.getClusterId())) {
                    logger.warn("[markdown] configured, markdown later in {} sec, {}", config.getDelaySecond(), info);
                    alertManager.alert(info, ALERT_TYPE.INSTANCE_SICK_BUT_DELAY_MARK_DOWN, info.getDcId());
                    return config;
                }
            }
        }
        return null;
    }

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
        DcClusterDelayMarkDown config = getDelayMarkDownIfConfiged(event);
        if(config != null) {
            scheduled.schedule(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() {
                    doRealMarkDown(event);
                }
            }, config.getDelaySecond(), TimeUnit.SECONDS);
        } else {
            doRealMarkDown(event);
        }
    }

    @VisibleForTesting
    public DefaultInstanceSickHandler setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }
}
