package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
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
            context.setTrueMaster(context.getAllMasters().iterator().next());
            logger.debug("[{}-{}+{}] {} true master in CheckTrueMaster step: {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), context.getTrueMaster());
            future().setSuccess();
        } else {
            logger.warn("[{}-{}+{}]collected masters not unique in CheckTrueMaster step: {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getAllMasters());
            Map<HostPort, Role> trueMastersMap = new ConcurrentHashMap<>();
            ParallelCommandChain roleCommandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
            context.getAllMasters().forEach(master -> roleCommandChain.add(roleCommand(trueMastersMap, master)));
            roleCommandChain.execute().addListener(outerFuture -> {
                try {
                    if (foundTrueMaster(trueMastersMap)) {
                        context.setTrueMaster(trueMastersMap.keySet().iterator().next());
                        logger.info("[{}-{}+{}] {} true master: {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), context.getTrueMaster());
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

    RoleCommand roleCommand(Map<HostPort, Role> trueMastersMap, HostPort master) {
        RoleCommand roleCommand = new RoleCommand(keyedObjectPool.getKeyPool(new DefaultEndPoint(master.getHost(), master.getPort())), scheduled);
        roleCommand.future().addListener(innerFuture -> {
            if (innerFuture.isSuccess()) {
                logger.info("[{}-{}+{}]instance {} role {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), master, innerFuture.get());
                if (innerFuture.get() instanceof MasterRole) {
                    trueMastersMap.put(master, innerFuture.get());
                }
            } else {
                logger.warn("[{}-{}+{}]instance {} role failed", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), master, innerFuture.cause());
            }
        });
        return roleCommand;
    }

    boolean foundTrueMaster(Map<HostPort, Role> trueMastersMap) {
        if (currentMasterConsistent(trueMastersMap.keySet())) {
            HostPort uniqueMaster = trueMastersMap.keySet().iterator().next();
            List<HostPort> slaves = ((MasterRole) trueMastersMap.get(uniqueMaster)).getSlaveHostPorts();
            List<HostPort> allShardInstances = context.getShardInstances();
            allShardInstances.remove(uniqueMaster);
            allShardInstances.removeAll(slaves);
            return allShardInstances.isEmpty();
        }
        return false;
    }

    CheckTrueMaster setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
        this.keyedObjectPool = keyedObjectPool;
        return this;
    }

    CheckTrueMaster setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }
}
