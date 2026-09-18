package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Triple;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class InfoReplIdAction extends AbstractHealthCheckAction<RedisHealthCheckInstance> {

    protected static final Logger logger = LoggerFactory.getLogger(InfoReplIdAction.class);

    private static final String MASTER_REPLID = "master_replid";

    private static final String MASTER_REPLID2 = "master_replid2";

    private final RedisSessionManager redisSessionManager;

    private final MetaCache metaCache;

    public InfoReplIdAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                            ExecutorService executors, RedisSessionManager redisSessionManager, MetaCache metaCache) {
        super(scheduled, instance, executors);
        this.redisSessionManager = redisSessionManager;
        this.metaCache = metaCache;
    }

    @Override
    protected void doTask() {
        try {
            InfoResultExtractor slaveInfo = instance.getRedisSession().syncInfo(InfoCommand.INFO_TYPE.REPLICATION);
            String slaveReplId = slaveInfo.extract(MASTER_REPLID);
            String masterHost = slaveInfo.getKeyKeeperMasterHost();
            int masterPort = slaveInfo.getKeyKeeperMasterPort();
            if (slaveReplId == null || masterHost == null || masterPort <= 0) {
                logger.info("[doTask][slave info incomplete] {}", instance.getCheckInfo().getHostPort());
                notifyListeners(new InfoReplIdActionContext(instance, new IllegalStateException("slave info incomplete")));
                return;
            }

            List<KeeperMeta> keepers = metaCache.getKeeperOfDcClusterShard(
                    instance.getCheckInfo().getDcId(),
                    instance.getCheckInfo().getClusterId(),
                    instance.getCheckInfo().getShardId());
            boolean keeperInMeta = keepers.stream()
                    .anyMatch(keeper -> masterHost.equals(keeper.getIp()) && masterPort == keeper.getPort());
            if (!keeperInMeta) {
                logger.info("[doTask][keeper not in meta] {} master={}:{}",
                        instance.getCheckInfo().getHostPort(), masterHost, masterPort);
                notifyListeners(new InfoReplIdActionContext(instance,
                        new KeeperNotInMetaException(String.format("keeper not in meta, master=%s:%d", masterHost, masterPort))));
                return;
            }

            RedisSession upstream = redisSessionManager.findOrCreateSession(new HostPort(masterHost, masterPort));
            InfoResultExtractor keeperInfo = upstream.syncInfo(InfoCommand.INFO_TYPE.REPLICATION);
            String keeperReplId = keeperInfo.extract(MASTER_REPLID);
            String keeperReplId2 = keeperInfo.extract(MASTER_REPLID2);
            if (keeperReplId == null) {
                logger.info("[doTask][keeper info incomplete] {}", instance.getCheckInfo().getHostPort());
                notifyListeners(new InfoReplIdActionContext(instance, new IllegalStateException("keeper info incomplete")));
                return;
            }

            boolean replIdMatch = slaveReplId.equals(keeperReplId)
                    || (keeperReplId2 != null && slaveReplId.equals(keeperReplId2));
            if(!replIdMatch) {
                logger.info("[doTask][replId match={}] {} slaveReplId={}, keeperReplId={}, keeperReplId2={}",
                        replIdMatch, instance.getCheckInfo().getHostPort(), slaveReplId, keeperReplId, keeperReplId2);
            }
            notifyListeners(new InfoReplIdActionContext(instance, new Triple<>(slaveReplId, keeperReplId, keeperReplId2)));
        } catch (Throwable th) {
            logger.info("[doTask][fail] {}", instance.getCheckInfo().getHostPort(), th);
            notifyListeners(new InfoReplIdActionContext(instance, th));
        }
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

}
