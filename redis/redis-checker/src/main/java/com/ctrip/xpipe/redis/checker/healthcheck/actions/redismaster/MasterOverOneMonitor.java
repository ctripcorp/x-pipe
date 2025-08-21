package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_KEYED_NETTY_CLIENT_POOL;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author lishanglin
 * date 2021/11/18
 */
@Component
public class MasterOverOneMonitor implements RedisMasterActionListener, OneWaySupport, BiDirectionSupport, SingleDcSupport, LocalDcSupport, CrossDcSupport {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    @Resource(name = REDIS_KEYED_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private static final Logger logger = LoggerFactory.getLogger(RedisWrongSlaveMonitor.class);

    private Map<DcClusterShard, Future<Boolean>> doubleCheckFutures;

    public MasterOverOneMonitor() {
        doubleCheckFutures = Maps.newConcurrentMap();
    }

    @Override
    public void onAction(RedisMasterActionContext redisMasterActionContext) {
        RedisInstanceInfo info = redisMasterActionContext.instance().getCheckInfo();

        if (!redisMasterActionContext.isSuccess()) {
            logger.debug("[{}][{}] role {} fail, skip", info.getClusterId(), info.getShardId(), info.getHostPort());
            return;
        }
        if (!Server.SERVER_ROLE.MASTER.equals(redisMasterActionContext.getResult().getServerRole())) return;

        String dcId = info.getDcId();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();
        HostPort master = info.getHostPort();
        ClusterType clusterType = info.getClusterType();
        Set<HostPort> otherMasters = findOtherMasters(dcId, clusterId, shardId, master, clusterType);
        if (otherMasters.isEmpty()) return;

        DcClusterShard dcClusterShard = new DcClusterShard(dcId, clusterId, shardId);
        Set<HostPort> masters = new HashSet<>();
        masters.add(master);
        masters.addAll(otherMasters);
        if (doubleCheckFutures.containsKey(dcClusterShard) && !doubleCheckFutures.get(dcClusterShard).isDone()) {
            logger.debug("[{}][{}][skip] multi master {}", info.getClusterId(), info.getShardId(), masters);
        } else {
            MapUtils.getOrCreate(doubleCheckFutures, dcClusterShard, () -> {
                CommandFuture<Boolean> doubleCheckFuture = (new MultiMasterDoubleCheckCmd(dcClusterShard, masters)).execute();
                doubleCheckFuture.addListener(checkResult -> {
                    if (checkResult.isSuccess() && Boolean.TRUE.equals(checkResult.get())) {
                        logger.info("[{}][{}] multi master {}", info.getClusterId(), info.getShardId(), masters);
                        alertManager.alert(dcClusterShard.getDcId(), dcClusterShard.getClusterId(), dcClusterShard.getShardId(),
                                null, ALERT_TYPE.MASTER_OVER_ONE, "multi masters:" + masters.toString());
                    } else {
                        logger.debug("[{}][{}][check fail] multi master {}", info.getClusterId(), info.getShardId(), masters);
                    }
                });
                return doubleCheckFuture;
            });
        }
    }

    public Set<HostPort> findOtherMasters(String dc, String cluster, String shard, HostPort master, ClusterType clusterType) {
        List<RedisMeta> redisMetas;
        if (clusterType.isCrossDc()) {
            redisMetas = metaCache.getAllInstancesOfShard(cluster, shard);
        } else {
            redisMetas = metaCache.getRedisOfDcClusterShard(dc, cluster, shard);
        }

        return redisMetas.stream()
                .filter(redisMeta -> redisMeta.isMaster() && !(redisMeta.getIp().equals(master.getHost())
                        && redisMeta.getPort().equals(master.getPort())))
                .map(redisMeta -> new HostPort(redisMeta.getIp(), redisMeta.getPort()))
                .collect(Collectors.toSet());
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        RedisInstanceInfo info = (RedisInstanceInfo) action.getActionInstance().getCheckInfo();
        DcClusterShard dcClusterShard = new DcClusterShard(info.getDcId(), info.getClusterId(), info.getShardId());
        doubleCheckFutures.remove(dcClusterShard);
    }

    private class MultiMasterDoubleCheckCmd extends AbstractCommand<Boolean> {

        private DcClusterShard dcClusterShard;

        private Set<HostPort> masters;

        public MultiMasterDoubleCheckCmd(DcClusterShard dcClusterShard, Set<HostPort> masters) {
            this.dcClusterShard = dcClusterShard;
            this.masters = masters;
        }

        @Override
        protected void doExecute() throws Throwable {
            if (masters.isEmpty() || 1 == masters.size()) future().setSuccess(false);

            ParallelCommandChain parallelCmds = new ParallelCommandChain();
            masters.forEach(master ->
                parallelCmds.add(new RoleCommand(
                                keyedObjectPool.getKeyPool(new DefaultEndPoint(master.getHost(), master.getPort())),
                                scheduled))
            );

            parallelCmds.execute().addListener(f -> {
                List<CommandFuture<?>> roleFutures = parallelCmds.getResult();
                Set<MasterRole> realMasters = new HashSet<>();
                roleFutures.forEach(roleFuture -> {
                    if (!roleFuture.isSuccess()) return;
                    try {
                        if ((roleFuture.get() instanceof MasterRole)) realMasters.add((MasterRole) roleFuture.get());
                    } catch (Throwable th) {
                        logger.debug("{} double check fail", getName(), th);
                    }
                });

                future().setSuccess(realMasters.size() > 1);
            });
        }

        @Override
        protected void doReset() {
        }

        @Override
        public String getName() {
            return String.format("multiMasterCheck[%s][%s][%s]",
                    dcClusterShard.getDcId(), dcClusterShard.getClusterId(), dcClusterShard.getShardId());
        }
    }

    @VisibleForTesting
    protected void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    @VisibleForTesting
    protected void setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
        this.keyedObjectPool = keyedObjectPool;
    }

}
