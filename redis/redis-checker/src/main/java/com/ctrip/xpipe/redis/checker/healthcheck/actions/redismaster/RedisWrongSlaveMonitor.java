package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2021/11/18
 */
@Component
public class RedisWrongSlaveMonitor implements RedisMasterActionListener, OneWaySupport, BiDirectionSupport {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private PingService pingService;

    @Autowired
    private AlertManager alertManager;

    private static final Logger logger = LoggerFactory.getLogger(RedisWrongSlaveMonitor.class);

    @Override
    public void onAction(RedisMasterActionContext redisMasterActionContext) {
        RedisInstanceInfo info = redisMasterActionContext.instance().getCheckInfo();

        if (!redisMasterActionContext.isSuccess()) {
            logger.debug("[onAction][{}][{}] role {} fail, skip", info.getClusterId(), info.getShardId(), info.getHostPort());
            return;
        }

        Role role = redisMasterActionContext.getResult();
        if (!(role instanceof MasterRole)) {
            return;
        }

        String dcId = info.getDcId();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();
        HostPort master = info.getHostPort();
        List<HostPort> realSlaves = ((MasterRole) role).getSlaveHostPorts();
        List<HostPort> expectedSlaves = findExpectedSlaves(dcId, clusterId, shardId);

        for (HostPort expectedSlave: expectedSlaves) {
            if (realSlaves.contains(expectedSlave)) continue;
            if (!pingService.isRedisAlive(expectedSlave)) {
                logger.debug("[{}][{}] {} expected slave of {} but down", clusterId, shardId, expectedSlave, master);
                continue;
            }
            logger.info("[{}][{}] {} expected slave of {} but not", clusterId, shardId, expectedSlave, master);
            alertManager.alert(dcId, clusterId, shardId, expectedSlave, ALERT_TYPE.REPL_WRONG_SLAVE,
                    String.format("expected slave of %s but not", master));
        }
    }

    public List<HostPort> findExpectedSlaves(String dc, String cluster, String shard) {
        List<RedisMeta> redisMetas = metaCache.getRedisOfDcClusterShard(dc, cluster, shard);
        return redisMetas.stream()
                .filter(redisMeta -> !redisMeta.isMaster())
                .map(redisMeta -> new HostPort(redisMeta.getIp(), redisMeta.getPort()))
                .collect(Collectors.toList());
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        // do nothing
    }
}
