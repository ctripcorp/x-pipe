package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
public abstract class AbstractHealthEventHandler<T extends AbstractInstanceEvent> implements HealthEventHandler {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    protected MetaCache metaCache;

    @Autowired
    protected AlertManager alertManager;

    @Autowired
    protected DefaultDelayPingActionCollector defaultDelayPingActionCollector;

    @Autowired
    private StabilityHolder siteStability;

    @Autowired
    private OuterClientAggregator outerClientAggregator;

    protected static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @SuppressWarnings("unchecked")
    @Override
    public void handle(AbstractInstanceEvent event) {
        if(!supports(event)) {
            return;
        }
        T t = (T) event;
        doHandle(t);
    }

    protected abstract void doHandle(T t);

    protected void tryMarkDown(AbstractInstanceEvent event) {
        if (!siteStability.isSiteStable()) {
            logger.warn("[onEvent][site down, skip] {}", event);
            return;
        }
        if(!event.getInstance().getCheckInfo().isCrossRegion() && !masterUp(event)) {
            logger.info("[onEvent][master down, do not call client service]{}", event);
            return;
        }
        markdown(event);
    }

    @VisibleForTesting
    protected void markdown(final AbstractInstanceEvent event) {
        final RedisInstanceInfo info = event.getInstance().getCheckInfo();
        if(siteStability.isSiteStable()) {
            doMarkDown(event);
        } else {
            logger.warn("[site-down][not-mark-down] {}", info);
        }
    }

    protected void doMarkDown(final AbstractInstanceEvent event) {
        //doNothing
    }

    protected void doRealMarkUp(final AbstractInstanceEvent event) {
        outerClientAggregator.markInstance(event.getInstance().getCheckInfo().getClusterShardHostport());
    }

    protected void doRealMarkDown(final AbstractInstanceEvent event) {
        final RedisInstanceInfo info = event.getInstance().getCheckInfo();
        if(stateUpNow(event)) {
            logger.warn("[markdown] instance state up now, do not mark down, {}", info);
        } else {
            logger.info("[markdown] mark down redis, {}, {}", event.getInstance().getCheckInfo(), event.getClass().getSimpleName());
            outerClientAggregator.markInstance(event.getInstance().getCheckInfo().getClusterShardHostport());
        }
    }

    private boolean stateUpNow(AbstractInstanceEvent event) {
        return defaultDelayPingActionCollector.getState(event.getInstance().getCheckInfo().getHostPort())
                .equals(HEALTH_STATE.HEALTHY);
    }

    protected boolean masterUp(AbstractInstanceEvent instanceEvent) {
        RedisInstanceInfo info = instanceEvent.getInstance().getCheckInfo();
        HostPort redisMaster = metaCache.findMasterInSameShard(info.getHostPort());
        HEALTH_STATE masterState = defaultDelayPingActionCollector.getState(redisMaster);

        if (HEALTH_STATE.UNHEALTHY.equals(masterState) || HEALTH_STATE.DOWN.equals(masterState) || HEALTH_STATE.SICK.equals(masterState)) {
            logger.info("[masterUp][master down instance:{}, master:{}] {}", info, redisMaster, masterState);
            return false;
        }
        return true;
    }

}
