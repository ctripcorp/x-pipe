package com.ctrip.xpipe.redis.console.health.redisconf.backlog;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisConf;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisConfManager;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisInfoUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Feb 05, 2018
 */
@Component
@Lazy
public class DefaultBacklogActiveCollector implements BacklogActiveCollector {

    private static Logger logger = LoggerFactory.getLogger(DefaultBacklogActiveCollector.class);

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private RedisConfManager redisConfManager;


    @Override
    public void collect(Sample<InstanceInfoReplicationResult> sample) {

        BacklogActiveSamplePlan samplePlan = (BacklogActiveSamplePlan) sample.getSamplePlan();
        String clusterId = samplePlan.getClusterId();
        String shardId = samplePlan.getShardId();

        samplePlan.getHostPort2SampleResult().forEach((hostPort, sampleResult) -> {
            if(sampleResult.isSuccess()) {

                String context = sampleResult.getContext();
                if(context == null || StringUtil.isEmpty(context)) {
                    logger.warn("[collect]Null String of Redis info, {} {} {}", clusterId, shardId, hostPort);
                    return;
                }
                try {
                    analysisInfoReplication(sampleResult.getContext(), clusterId, shardId, hostPort);
                } catch (Exception e) {
                    logger.error("[collect]", e);
                }
            } else {
                logger.error("[collect]get Redis info replication, execution error: {}", sampleResult.getFailReason());
            }
        });
    }

    @VisibleForTesting
    void analysisInfoReplication(String infoReplication, String cluster, String shard, HostPort hostPort) {
        boolean isBacklogActive = RedisInfoUtils.getReplBacklogActive(infoReplication);
        String role = RedisInfoUtils.getRole(infoReplication);
        if(!isBacklogActive && Server.SERVER_ROLE.SLAVE.sameRole(role)) {
            // master sync in progress == REPL_STATE_TRANSFER
            // master last io seconds ago == server.master ? unix time - last ! -1
            if(RedisInfoUtils.isMasterSyncInProgress(infoReplication)
                    || RedisInfoUtils.getMasterLastIoSecondsAgo(infoReplication) == -1) {

                logger.info("[analysisInfoReplication] master sync in progress, waiting for {}, {}, {}",
                        cluster, shard, hostPort);
                return;
            }
            RedisConf redisConf = redisConfManager.findOrCreateConfig(hostPort.getHost(), hostPort.getPort());
            if((!StringUtil.isEmpty(redisConf.getRedisVersion())
                    && StringUtil.compareVersion(redisConf.getRedisVersion(), "4.0.0") >= 0)
                    || !StringUtil.isEmpty(redisConf.getXredisVersion())) {

                String message = "Redis replication backlog not active";
                alertManager.alert(cluster, shard, hostPort, ALERT_TYPE.REPL_BACKLOG_NOT_ACTIVE, message);
            } else {
                logger.warn("[analysisInfoReplication]Redis {}-{}-{} backlog_active is 0, " +
                        "but redis is not xredis or with version greater than 4.0.0", cluster, shard, hostPort);
            }
        }
    }
}
