package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.concurrent.FinalStateSetterManager;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
@Component
@Lazy
public class OuterClientServiceProcessor implements HealthEventProcessor {

    private OuterClientService outerClientService = OuterClientService.DEFAULT;

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private AllMonitorCollector allMonitorCollector;

    @Autowired
    private ConsoleServiceManager consoleServiceManager;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    private FinalStateSetterManager<ClusterShardHostPort, Boolean> finalStateSetterManager;

    @PostConstruct
    public void postConstruct() {

        finalStateSetterManager = new FinalStateSetterManager<>(executors, (clusterShardHostPort) -> {

            try {
                return outerClientService.isInstanceUp(clusterShardHostPort);
            } catch (OuterClientException e) {
                throw new IllegalStateException("get error:" + clusterShardHostPort, e);
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

    @Override
    public void onEvent(AbstractInstanceEvent instanceEvent) throws HealthEventProcessorException {

        HostPort hostPort = instanceEvent.getHostPort();
        Pair<String, String> clusterShard = metaCache.findClusterShard(hostPort);

        if (!instanceInBackupDc(hostPort)) {
            logger.info("[onEvent][instance not in backupDc]{}, {}", clusterShard, hostPort);
            return;
        }

        ClusterShardHostPort clusterShardHostPort = new ClusterShardHostPort(hostPort);
        if(clusterShard != null){
            clusterShardHostPort.setClusterName(clusterShard.getKey());
            clusterShardHostPort.setShardName(clusterShard.getValue());
        }

        if (instanceEvent instanceof InstanceUp) {
            finalStateSetterManager.set(clusterShardHostPort, true);
        } else if (instanceEvent instanceof InstanceDown) {

            if (masterUp(clusterShardHostPort)) {
                quorumMarkInstanceDown(clusterShardHostPort);
            } else {
                logger.info("[onEvent][master down, do not call client service]{}", instanceEvent);
            }
        } else {
            throw new IllegalStateException("unknown event:" + instanceEvent);
        }
    }

    private void quorumMarkInstanceDown(ClusterShardHostPort clusterShardHostPort) {

        HostPort hostPort = clusterShardHostPort.getHostPort();

        List<HEALTH_STATE> health_states = consoleServiceManager.allHealthStatus(hostPort.getHost(), hostPort.getPort());

        logger.info("[quorumMarkInstanceDown]{}, {}", clusterShardHostPort, health_states);

        boolean quorum = consoleServiceManager.quorumSatisfy(health_states,
                (state) -> state == HEALTH_STATE.UNHEALTHY || state == HEALTH_STATE.DOWN);

        if (quorum) {
            finalStateSetterManager.set(clusterShardHostPort, false);
        } else {
            logger.info("[quorumMarkInstanceDown][quorum fail]{}, {}", clusterShardHostPort, quorum);
            alertManager.alert(
                    clusterShardHostPort.getClusterName(),
                    clusterShardHostPort.getShardName(),
                    hostPort,
                    ALERT_TYPE.QUORUM_DOWN_FAIL,
                    hostPort.toString()
            );
        }
    }

    private boolean instanceInBackupDc(HostPort hostPort) {
        return metaCache.inBackupDc(hostPort);
    }

    private boolean masterUp(ClusterShardHostPort clusterShardHostPort) {

        //master up
        HostPort redisMaster = metaCache.findMasterInSameShard(clusterShardHostPort.getHostPort());
        boolean masterUp = allMonitorCollector.getState(redisMaster) == HEALTH_STATE.UP;
        if (!masterUp) {
            logger.info("[masterUp][master down instance:{}, master:{}]", clusterShardHostPort, redisMaster);
        }
        return masterUp;
    }
}