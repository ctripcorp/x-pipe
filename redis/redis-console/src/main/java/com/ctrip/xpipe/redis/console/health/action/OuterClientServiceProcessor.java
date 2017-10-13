package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.concurrent.FinalStateSetterManager;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
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
    private AllMonitorCollector allMonitorCollector;

    @Autowired
    private ConsoleServiceManager consoleServiceManager;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    private FinalStateSetterManager<HostPort, Boolean> finalStateSetterManager;

    @PostConstruct
    public void postConstruct() {

        finalStateSetterManager = new FinalStateSetterManager<>(executors, (hostPort) -> {

            try {
                return outerClientService.isInstanceUp(hostPort);
            } catch (OuterClientException e) {
                throw new IllegalStateException("get error:" + hostPort, e);
            }
        }, ((hostPort, result) -> {
            try {
                if (result) {
                    outerClientService.markInstanceUp(hostPort);
                } else {
                    outerClientService.markInstanceDown(hostPort);
                }
            } catch (OuterClientException e) {
                throw new IllegalStateException("set error:" + hostPort + "," + result, e);
            }
        })
        );

    }

    @Override
    public void onEvent(AbstractInstanceEvent instanceEvent) throws HealthEventProcessorException {

        if (!instanceInBackupDc(instanceEvent.getHostPort())) {
            logger.info("[onEvent][instance not in backupDc]{}", instanceEvent.getHostPort());
            return;
        }

        if (instanceEvent instanceof InstanceUp) {
            finalStateSetterManager.set(instanceEvent.getHostPort(), true);
        } else if (instanceEvent instanceof InstanceDown) {

            if (masterUp(instanceEvent.getHostPort())) {
                quorumMarkInstanceDown(instanceEvent.getHostPort());
            } else {
                logger.info("[onEvent][master down, do not call client service]{}", instanceEvent);
            }
        } else {
            throw new IllegalStateException("unknown event:" + instanceEvent);
        }
    }

    private void quorumMarkInstanceDown(HostPort hostPort) {

        List<HEALTH_STATE> health_states = consoleServiceManager.allHealthStatus(hostPort.getHost(), hostPort.getPort());

        logger.info("[quorumMarkInstanceDown]{}, {}", hostPort, health_states);

        boolean quorum = consoleServiceManager.quorumSatisfy(health_states,
                (state) -> state == HEALTH_STATE.UNHEALTHY || state == HEALTH_STATE.DOWN);

        if (quorum) {
            finalStateSetterManager.set(hostPort, false);
        } else {
            logger.info("[quorumMarkInstanceDown][quorum fail]{}, {}", hostPort, quorum);
            CatEventMonitor.DEFAULT.logAlertEvent("quorum_fail:" + hostPort);
        }
    }

    private boolean instanceInBackupDc(HostPort hostPort) {
        return metaCache.inBackupDc(hostPort);
    }

    private boolean masterUp(HostPort hostPort) {

        //master up
        HostPort redisMaster = metaCache.findMasterInSameShard(hostPort);
        boolean masterUp = allMonitorCollector.getState(redisMaster) == HEALTH_STATE.UP;
        if (!masterUp) {
            logger.info("[masterUp][master down instance:{}, master:{}]", hostPort, redisMaster);
        }
        return masterUp;
    }
}