package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.cluster.ClusterServer;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.ClusterHealthManager;
import com.ctrip.xpipe.redis.checker.CrossMasterDelayManager;
import com.ctrip.xpipe.redis.checker.RedisDelayManager;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderAware;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.model.CheckerRole;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResult;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class HealthCheckReporter implements GroupCheckerLeaderAware {

    private HealthStateService healthStateService;

    private RedisDelayManager redisDelayManager;

    private CrossMasterDelayManager crossMasterDelayManager;

    private PingService pingService;

    private ClusterHealthManager clusterHealthManager;

    private CheckerConsoleService checkerConsoleService;

    private ClusterServer clusterServer;
    
    private ClusterServer allCheckerServer;

    private CheckerConfig config;

    private DynamicDelayPeriodTask heartbeatTask;

    private DynamicDelayPeriodTask checkResultReportTask;

    private ScheduledExecutorService scheduled;

    private int serverPort;

    private static final String CURRENT_IDC = FoundationService.DEFAULT.getDataCenter();

    private static final Logger logger = LoggerFactory.getLogger(HealthCheckReporter.class);

    public HealthCheckReporter(HealthStateService healthStateService, CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService,
                               ClusterServer clusterServer, ClusterServer allCheckerServer, RedisDelayManager redisDelayManager,
                               CrossMasterDelayManager crossMasterDelayManager, PingService pingService,
                               ClusterHealthManager clusterHealthManager, int serverPort) {
        this.healthStateService = healthStateService;
        this.serverPort = serverPort;
        this.config = checkerConfig;
        this.checkerConsoleService = checkerConsoleService;
        this.clusterServer = clusterServer;
        this.allCheckerServer = allCheckerServer;
        this.redisDelayManager = redisDelayManager;
        this.crossMasterDelayManager = crossMasterDelayManager;
        this.pingService = pingService;
        this.clusterHealthManager = clusterHealthManager;
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("CheckerReporter"));
        this.heartbeatTask = new DynamicDelayPeriodTask("CheckerHeartbeat", this::heartbeat, config::getCheckerAckIntervalMilli, scheduled);
        this.checkResultReportTask = new DynamicDelayPeriodTask("CheckerReporter", this::reportCheckResult, config::getCheckerReportIntervalMilli, scheduled);
    }

    @PostConstruct
    public void postConstruct() {
        try {
            heartbeatTask.start();
        } catch (Throwable th) {
            logger.info("[postConstruct] heartbeatTask start fail", th);
        }
    }

    @PreDestroy
    public void preDestroy() {
        try {
            heartbeatTask.stop();
            checkResultReportTask.stop();
            this.scheduled.shutdownNow();
        } catch (Throwable th) {
            logger.info("[preDestroy] fail", th);
        }
    }

    @Override
    public void isleader() {
        try {
            logger.debug("[isleader] become leader");
            checkResultReportTask.start();
        } catch (Throwable th) {
            logger.info("[isleader] checkResultReportTask start fail", th);
        }
    }

    @Override
    public void notLeader() {
        try {
            logger.debug("[notLeader] loss leader");
            checkResultReportTask.stop();
        } catch (Throwable th) {
            logger.info("[postConstruct] checkResultReportTask stop fail", th);
        }
    }

    private void heartbeat() {
        try {
            logger.debug("[heartbeat] start");
            CheckerStatus status = new CheckerStatus();
            status.setHostPort(new HostPort(FoundationService.DEFAULT.getLocalIp(), serverPort));
            status.setCheckerRole(clusterServer.amILeader() ? CheckerRole.LEADER : CheckerRole.FOLLOWER);
            status.setAllCheckerRole(allCheckerServer.amILeader()? CheckerRole.LEADER: CheckerRole.FOLLOWER);
            status.setPartIndex(config.getClustersPartIndex());

            checkerConsoleService.ack(config.getConsoleAddress(), status);
        } catch (Throwable th) {
            logger.info("[heartbeat] fail", th);
        }
    }

    private void reportCheckResult() {
        try {
            logger.debug("[reportCheckResult] start");
            HealthCheckResult result = new HealthCheckResult();
            result.encodeRedisDelays(redisDelayManager.getAllDelays());
            result.encodeCrossMasterDelays(crossMasterDelayManager.getAllCrossMasterDelays());
            result.encodeRedisAlives(pingService.getAllRedisAlives());
            result.setWarningClusterShards(clusterHealthManager.getAllClusterWarningShards());
            result.encodeRedisStates(healthStateService.getAllCachedState());
            result.setHeteroShardsDelay(redisDelayManager.getAllHeteroShardsDelays());

            checkerConsoleService.report(config.getConsoleAddress(), result);
        } catch (Throwable th) {
            logger.info("[reportCheckResult] fail", th);
        }
    }

}
