package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class InfoReplIdAction extends AbstractHealthCheckAction<RedisHealthCheckInstance> {

    protected static final Logger logger = LoggerFactory.getLogger(InfoReplIdAction.class);

    private static final String MASTER_REPLID = "master_replid";

    private final RedisSessionManager redisSessionManager;

    public InfoReplIdAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                            ExecutorService executors, RedisSessionManager redisSessionManager) {
        super(scheduled, instance, executors);
        this.redisSessionManager = redisSessionManager;
    }

    @Override
    protected void doTask() {
        try {
            InfoResultExtractor slaveInfo = instance.getRedisSession().syncInfo(InfoCommand.INFO_TYPE.REPLICATION);
            String slaveReplId = slaveInfo.extract(MASTER_REPLID);
            String masterHost = slaveInfo.getKeyKeeperMasterHost();
            int masterPort = slaveInfo.getKeyKeeperMasterPort();
            logger.info("[doTask][slave] {} slaveReplId={}, master={}:{}",
                    instance.getCheckInfo().getHostPort(), slaveReplId, masterHost, masterPort);
            if (slaveReplId == null || masterHost == null || masterPort <= 0) {
                logger.info("[doTask][slave info incomplete] {}", instance.getCheckInfo().getHostPort());
                notifyListeners(new InfoReplIdActionContext(instance, new IllegalStateException("slave info incomplete")));
                return;
            }

            RedisSession upstream = redisSessionManager.findOrCreateSession(new HostPort(masterHost, masterPort));
            String keeperReplId = upstream.syncInfo(InfoCommand.INFO_TYPE.REPLICATION).extract(MASTER_REPLID);
            logger.info("[doTask][upstream] {} keeperReplId={}", instance.getCheckInfo().getHostPort(), keeperReplId);
            if (keeperReplId == null) {
                logger.info("[doTask][keeper info incomplete] {}", instance.getCheckInfo().getHostPort());
                notifyListeners(new InfoReplIdActionContext(instance, new IllegalStateException("keeper info incomplete")));
                return;
            }

            logger.info("[doTask][replId match={}] {} slaveReplId={}, keeperReplId={}",
                    slaveReplId.equals(keeperReplId), instance.getCheckInfo().getHostPort(), slaveReplId, keeperReplId);
            notifyListeners(new InfoReplIdActionContext(instance, new Pair<>(slaveReplId, keeperReplId)));
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
