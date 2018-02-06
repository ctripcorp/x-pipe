package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.health.delay.DelayCollector;
import com.ctrip.xpipe.redis.console.health.delay.DelaySampleResult;
import com.ctrip.xpipe.redis.console.health.ping.PingCollector;
import com.ctrip.xpipe.redis.console.health.ping.PingSampleResult;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
@Component
@ConditionalOnProperty(name = { HealthChecker.ENABLED }, matchIfMissing = true)
public class AllMonitorCollector implements PingCollector, DelayCollector{

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private final int  THREAD_COUNT = 4;
    private final int  CORE_POOL_MONITOR_PROCESS = 100;

    private Map<HostPort, HealthStatus> allHealthStatus = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduled;

    private ExecutorService executors;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private List<HealthEventProcessor> healthEventProcessors;

    @PostConstruct
    public void postConstruct(){
        scheduled = Executors.newScheduledThreadPool(THREAD_COUNT, XpipeThreadFactory.create("ALL_MONITOR_CHECK"));
        executors = DefaultExecutorFactory.createAllowCoreTimeoutAbortPolicy("ALL_MONITOR_PRO", CORE_POOL_MONITOR_PROCESS).createExecutorService();

    }

    public HEALTH_STATE getState(HostPort hostPort){

        if(hostPort == null || StringUtil.isEmpty(hostPort.getHost())) {
            return HEALTH_STATE.UNKNOWN;
        }
        HealthStatus healthStatus = allHealthStatus.get(hostPort);
        if(healthStatus == null){
            return HEALTH_STATE.UNKNOWN;
        }
        return healthStatus.getState();
    }


    @PreDestroy
    public void preDestroy(){
        scheduled.shutdownNow();
        executors.shutdownNow();
    }


    @Override
    public void collect(PingSampleResult result) {

        for (Map.Entry<HostPort, Boolean> entry : result.getSlaveHostPort2Pong().entrySet()) {

            HealthStatus healthStatus = createOrGet(entry.getKey());
            if(entry.getValue()){
                healthStatus.pong();
            }
        }
    }

    private HealthStatus createOrGet(HostPort key) {

        return MapUtils.getOrCreate(allHealthStatus, key, new ObjectFactory<HealthStatus>() {
            @Override
            public HealthStatus create() {

                HealthStatus healthStatus = new HealthStatus(
                        key,
                        () -> consoleConfig.getDownAfterCheckNums() * consoleConfig.getRedisReplicationHealthCheckInterval(),
                        () -> consoleConfig.getHealthyDelayMilli(),
                        scheduled);

                healthStatus.addObserver(new Observer() {
                    @Override
                    public void update(Object args, Observable observable) {
                        onInstanceStateChange(args);
                    }
                });
                return healthStatus;
            }
        });
    }

    @Override
    public void collect(DelaySampleResult result) {
        HealthStatus healthStatus;
        if(result.getMasterHostPort() != null) {
            healthStatus = createOrGet(result.getMasterHostPort());
            healthStatus.delay(TimeUnit.NANOSECONDS.toMillis(result.getMasterDelayNanos()));
        }
        for (Map.Entry<HostPort, Long> entry : result.getSlaveHostPort2Delay().entrySet()) {

            healthStatus = createOrGet(entry.getKey());
            healthStatus.delay(TimeUnit.NANOSECONDS.toMillis(entry.getValue()));
        }
    }

    protected void onInstanceStateChange(Object args) {

        logger.info("[onInstanceStateChange]{}", args);

        //TODO delete instance not exist

        for(HealthEventProcessor processor : healthEventProcessors){

            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    processor.onEvent((AbstractInstanceEvent) args);
                }
            });
        }
    }
}
