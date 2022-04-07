package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.MasterSlavesInfo;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.protocal.pojo.RedisInfo;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class CheckTrueMaster extends AbstractSentinelHelloCollectCommand {

    private AlertManager alertManager;
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;
    private ScheduledExecutorService scheduled;

    public CheckTrueMaster(SentinelHelloCollectContext context, AlertManager alertManager,
                           XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled) {
        super(context);
        this.alertManager = alertManager;
        this.keyedObjectPool = keyedObjectPool;
        this.scheduled = scheduled;
    }

    @Override
    protected void doExecute() throws Throwable {
        collectAllMasters();

        if (currentMasterConsistent(context.getAllMasters())) {
            HostPort trueMaster = context.getAllMasters().iterator().next();
            List<HostPort> shardSlaves = context.getShardInstances();
            shardSlaves.remove(trueMaster);
            context.setTrueMasterInfo(new Pair<>(trueMaster, shardSlaves));
            logger.debug("[{}-{}+{}] {} true master in CheckTrueMaster step: {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), context.getTrueMasterInfo().getKey());
            future().setSuccess();
        } else {
            logger.warn("[{}-{}+{}]collected masters not unique in CheckTrueMaster step: {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getAllMasters());
            Map<HostPort, List<HostPort>> trueMastersMap = new ConcurrentHashMap<>();
            ParallelCommandChain roleCommandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
            context.getAllMasters().forEach(master -> roleCommandChain.add(checkMasterRoleAndSlaves(trueMastersMap, master)));
            roleCommandChain.execute().addListener(outerFuture -> {
                try {
                    if (currentMasterConsistent(trueMastersMap.keySet())) {
                        HostPort trueMaster = trueMastersMap.keySet().iterator().next();
                        List<HostPort> slaves = trueMastersMap.get(trueMaster);
                        context.setTrueMasterInfo(new Pair<>(trueMaster, slaves));
                        logger.info("[{}-{}+{}] {} true master info: {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), context.getTrueMasterInfo());
                        future().setSuccess();
                    } else {
                        logger.warn("[{}-{}+{}] {} master not found: {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), trueMastersMap.keySet());
                        String message = String.format("master inconsistent, monitorName:%s, masters:%s", context.getSentinelMonitorName(), trueMastersMap.keySet());
                        alertManager.alert(context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getInfo().getHostPort(), ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, message);
                        future().setFailure(new MasterNotFoundException(context.getInfo().getClusterId(), context.getInfo().getShardId()));
                    }
                } catch (Throwable e) {
                    logger.error("[{}-{}+{}]CheckTrueMaster failed,", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), e);
                    future().setFailure(e);
                }
            });
        }
    }

    CheckMasterRoleAndSlaves checkMasterRoleAndSlaves(Map<HostPort, List<HostPort>> trueMastersMap, HostPort master) {
        CheckMasterRoleAndSlaves checkMasterRoleAndSlaves = new CheckMasterRoleAndSlaves(keyedObjectPool.getKeyPool(new DefaultEndPoint(master.getHost(), master.getPort())), scheduled);
        checkMasterRoleAndSlaves.future().addListener(innerFuture -> {
            if (innerFuture.isSuccess()) {
                RedisInfo redisInfo = innerFuture.get();
                logger.info("[{}-{}+{}]instance {} role {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), master, redisInfo.getRole());
                if (redisInfo instanceof MasterSlavesInfo) {
                    trueMastersMap.put(master, ((MasterSlavesInfo) redisInfo).getSlaves());
                }
            } else {
                logger.warn("[{}-{}+{}]instance {} role failed", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), master, innerFuture.cause());
            }
        });
        return checkMasterRoleAndSlaves;
    }

    @VisibleForTesting
    CheckTrueMaster setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
        this.keyedObjectPool = keyedObjectPool;
        return this;
    }

    @VisibleForTesting
    CheckTrueMaster setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }
}
