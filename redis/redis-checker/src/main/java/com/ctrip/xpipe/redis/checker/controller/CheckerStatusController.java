package com.ctrip.xpipe.redis.checker.controller;

import com.ctrip.xpipe.api.cluster.ClusterServer;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.ClusterHealthManager;
import com.ctrip.xpipe.redis.checker.CrossMasterDelayManager;
import com.ctrip.xpipe.redis.checker.RedisDelayManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.model.CheckerRole;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResult;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author lishanglin
 * date 2021/3/18
 */
@RestController
@RequestMapping("/api/checker")
public class CheckerStatusController {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CheckerConfig config;

    @Autowired
    private ClusterServer clusterServer;

    @Autowired
    private RedisDelayManager redisDelayManager;

    @Autowired
    private CrossMasterDelayManager crossMasterDelayManager;

    @Autowired
    private PingService pingService;

    @Autowired
    private ClusterHealthManager clusterHealthManager;

    @Value("${server.port}")
    private int serverPort;

    @GetMapping("/status")
    public CheckerStatus getCheckerStatus() {
        CheckerStatus status = new CheckerStatus();
        status.setPartIndex(config.getClustersPartIndex());
        status.setCheckerRole(clusterServer.amILeader() ? CheckerRole.LEADER : CheckerRole.FOLLOWER);
        status.setHostPort(new HostPort(FoundationService.DEFAULT.getLocalIp(), serverPort));

        return status;
    }

    @GetMapping("/meta")
    public XpipeMeta getMeta() {
        return metaCache.getXpipeMeta();
    }

    @GetMapping("/result")
    public HealthCheckResult getCheckResult() {
        HealthCheckResult result = new HealthCheckResult();
        result.setRedisDelays(redisDelayManager.getAllDelays());
        result.setCrossMasterDelays(crossMasterDelayManager.getAllCrossMasterDelays());
        result.setRedisAlives(pingService.getAllRedisAlives());
        result.setWarningClusterShards(clusterHealthManager.getAllClusterWarningShards());

        return result;
    }

}
