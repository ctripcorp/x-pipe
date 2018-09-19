package com.ctrip.xpipe.redis.console.healthcheck.action.handler;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.concurrent.FinalStateSetterManager;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.action.DelayPingActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.action.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.healthcheck.action.SiteReliabilityChecker;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

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
    protected DelayPingActionListener delayPingActionListener;

    @Autowired
    private ConsoleServiceManager consoleServiceManager;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    @Autowired
    private SiteReliabilityChecker checker;

    private OuterClientService outerClientService = OuterClientService.DEFAULT;

    protected FinalStateSetterManager<ClusterShardHostPort, Boolean> finalStateSetterManager;

    @PostConstruct
    public void postConstruct() {

        finalStateSetterManager = new FinalStateSetterManager<>(executors, (clusterShardHostPort) -> {

            try {
                return outerClientService.isInstanceUp(clusterShardHostPort);
            } catch (OuterClientException e) {
                throw new IllegalStateException("findRedisHealthCheckInstance error:" + clusterShardHostPort, e);
            }
        }, ((clusterShardHostPort, result) -> {
            try {
                if (result) {
                    outerClientService.markInstanceUp(clusterShardHostPort);
                    alertManager.alert(clusterShardHostPort.getClusterName(), clusterShardHostPort.getShardName(),
                            clusterShardHostPort.getHostPort(), ALERT_TYPE.MARK_INSTANCE_UP, "Mark Instance Up");
                } else {
                    outerClientService.markInstanceDown(clusterShardHostPort);
                    alertManager.alert(clusterShardHostPort.getClusterName(), clusterShardHostPort.getShardName(),
                            clusterShardHostPort.getHostPort(), ALERT_TYPE.MARK_INSTANCE_DOWN, "Mark Instance Down");
                }
            } catch (OuterClientException e) {
                throw new IllegalStateException("set error:" + clusterShardHostPort + "," + result, e);
            }
        })
        );

    }

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

    protected boolean configedNotMarkDown(AbstractInstanceEvent event) {
        return false;
    }

    protected void tryMarkDown(AbstractInstanceEvent event) {
        if(!masterUp(event)) {
            logger.info("[onEvent][master down, do not call client service]{}", event);
            return;
        }
        quorumMarkInstanceDown(event);
    }

    protected void quorumMarkInstanceDown(AbstractInstanceEvent event) {
        RedisInstanceInfo info = event.getInstance().getRedisInstanceInfo();
        boolean quorum = quorumState(getSatisfiedStates(), info.getHostPort());

        if (quorum) {
            markdown(event);
        } else {
            logger.info("[quorumMarkInstanceDown][quorum fail]{}, {}", info.getClusterShardHostport(), quorum);
            alertManager.alert(
                    info.getClusterId(),
                    info.getShardId(),
                    info.getHostPort(),
                    ALERT_TYPE.QUORUM_DOWN_FAIL,
                    info.getHostPort().toString()
            );
        }
    }

    @VisibleForTesting
    protected void markdown(final AbstractInstanceEvent event) {
        final RedisInstanceInfo info = event.getInstance().getRedisInstanceInfo();
        checker.check(event).addListener(new CommandFutureListener<Boolean>() {
            @Override
            public void operationComplete(CommandFuture<Boolean> commandFuture) throws Exception {
                boolean siteReliable = commandFuture.get();
                if(siteReliable) {
                    if(!configedNotMarkDown(event)) {
                        finalStateSetterManager.set(info.getClusterShardHostport(), false);
                    }
                } else {
                    logger.warn("[site-down][not-mark-down] {}", info);
                }
            }
        });
    }

    protected boolean masterUp(AbstractInstanceEvent instanceEvent) {
        RedisInstanceInfo info = instanceEvent.getInstance().getRedisInstanceInfo();
        HostPort redisMaster = metaCache.findMasterInSameShard(info.getHostPort());
        boolean masterUp = delayPingActionListener.getState(redisMaster) == HEALTH_STATE.UP;
        if (!masterUp) {
            logger.info("[masterUp][master down instance:{}, master:{}]", info, redisMaster);
        }
        return masterUp;
    }

    protected boolean quorumState(List<HEALTH_STATE> healthStates, HostPort hostPort) {
        List<HEALTH_STATE> health_states = consoleServiceManager.allHealthStatus(hostPort.getHost(), hostPort.getPort());
        return consoleServiceManager.quorumSatisfy(health_states, (state) -> healthStates.contains(state));
    }

    @VisibleForTesting
    public void setFinalStateSetterManager(FinalStateSetterManager<ClusterShardHostPort, Boolean> finalStateSetterManager) {
        this.finalStateSetterManager = finalStateSetterManager;
    }

    @VisibleForTesting
    public void setChecker(SiteReliabilityChecker checker) {
        this.checker = checker;
    }
}
