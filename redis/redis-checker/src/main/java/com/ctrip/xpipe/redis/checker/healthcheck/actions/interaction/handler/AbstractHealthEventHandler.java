package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.concurrent.FinalStateSetterManager;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceLoading;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.List;
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
    private RemoteCheckerManager remoteCheckerManager;

    @Autowired
    private StabilityHolder siteStability;

    @Autowired
    private CheckerConfig checkerConfig;

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

    @SuppressWarnings("unchecked")
    protected List<HEALTH_STATE> getSatisfiedStates(){
        return Collections.EMPTY_LIST;
    }

    protected void tryMarkDown(AbstractInstanceEvent event) {
        if (siteStability.isSiteStable()) {
            logger.warn("[onEvent][site down, skip] {}", event);
        }
        if(!masterUp(event)) {
            logger.info("[onEvent][master down, do not call client service]{}", event);
            return;
        }
        if (event instanceof InstanceLoading) {
            markdown(event);
        } else {
            quorumMarkInstanceDown(event);
        }
    }

    protected void quorumMarkInstanceDown(AbstractInstanceEvent event) {
        RedisInstanceInfo info = event.getInstance().getCheckInfo();
        boolean quorum = quorumState(getSatisfiedStates(), info.getHostPort());

        if (quorum) {
            markdown(event);
        } else {
            logger.info("[quorumMarkInstanceDown][quorum fail]{}, {}", info.getClusterShardHostport(), quorum);
            alertManager.alert(info, ALERT_TYPE.QUORUM_DOWN_FAIL, info.getHostPort().toString());
        }
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
        getHealthStateSetterManager().set(event.getInstance().getCheckInfo().getClusterShardHostport(), true);
    }

    protected void doRealMarkDown(final AbstractInstanceEvent event) {
        final RedisInstanceInfo info = event.getInstance().getCheckInfo();
        if(stateUpNow(event)) {
            logger.warn("[markdown] instance state up now, do not mark down, {}", info);
        } else {
            logger.info("[markdown] mark down redis, {}, {}", event.getInstance().getCheckInfo(), event.getClass().getSimpleName());
            getHealthStateSetterManager().set(info.getClusterShardHostport(), false);
        }
    }

    private boolean stateUpNow(AbstractInstanceEvent event) {
        return defaultDelayPingActionCollector.getState(event.getInstance().getCheckInfo().getHostPort())
                .equals(HEALTH_STATE.HEALTHY);
    }

    protected boolean masterUp(AbstractInstanceEvent instanceEvent) {
        RedisInstanceInfo info = instanceEvent.getInstance().getCheckInfo();
        HostPort redisMaster = metaCache.findMasterInSameShard(info.getHostPort());
        boolean masterUp = defaultDelayPingActionCollector.getState(redisMaster) == HEALTH_STATE.HEALTHY;
        if (!masterUp) {
            logger.info("[masterUp][master down instance:{}, master:{}]", info, redisMaster);
        }
        return masterUp;
    }

    protected boolean quorumState(List<HEALTH_STATE> healthStates, HostPort hostPort) {
        List<HEALTH_STATE> health_states = remoteCheckerManager.getHealthStates(hostPort.getHost(), hostPort.getPort());
        long matchStates = health_states.stream().filter(healthStates::contains).count();
        return matchStates >= checkerConfig.getQuorum();
    }

    private FinalStateSetterManager<ClusterShardHostPort, Boolean> getHealthStateSetterManager() {
        return defaultDelayPingActionCollector.getHealthStateSetterManager();
    }
}
