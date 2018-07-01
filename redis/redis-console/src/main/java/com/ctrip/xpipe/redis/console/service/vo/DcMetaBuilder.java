package com.ctrip.xpipe.redis.console.service.vo;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Apr 03, 2018
 */
public class DcMetaBuilder extends AbstractCommand<DcMeta> {

    private DcMeta dcMeta;

    private Map<Long, String> dcNameMap;

    private RedisMetaService redisMetaService;

    private DcClusterService dcClusterService;

    private ClusterMetaService clusterMetaService;

    private DcClusterShardService dcClusterShardService;

    private DcService dcService;

    private ExecutorService executors;

    private RetryCommandFactory factory;

    private long dcId;

    private Map<Long, List<DcClusterTbl>> cluster2DcClusterMap;

    private List<DcClusterShardTbl> dcClusterShards;

    public DcMetaBuilder(DcMeta dcMeta, long dcId, ExecutorService executors, RedisMetaService redisMetaService, DcClusterService dcClusterService,
                         ClusterMetaService clusterMetaService, DcClusterShardService dcClusterShardService, DcService dcService, RetryCommandFactory factory) {
        this.dcMeta = dcMeta;
        this.dcId = dcId;
        this.executors = executors;
        this.redisMetaService = redisMetaService;
        this.dcClusterService = dcClusterService;
        this.clusterMetaService = clusterMetaService;
        this.dcClusterShardService = dcClusterShardService;
        this.dcService = dcService;
        this.factory = factory;
    }

    @Override
    protected void doExecute() throws Exception {
        SequenceCommandChain sequenceCommandChain = new SequenceCommandChain(false);

        ParallelCommandChain parallelCommandChain = new ParallelCommandChain(executors);
        parallelCommandChain.add(retry3TimesUntilSuccess(new GetAllDcClusterShardDetailCommand(dcId)));
        parallelCommandChain.add(retry3TimesUntilSuccess(new Cluster2DcClusterMapCommand()));
        parallelCommandChain.add(retry3TimesUntilSuccess(new GetDcIdNameMapCommand()));

        sequenceCommandChain.add(parallelCommandChain);
        sequenceCommandChain.add(retry3TimesUntilSuccess(new BuildDcMetaCommand()));

        logger.info("[doExecute] commands: {}", sequenceCommandChain);

        sequenceCommandChain.future().addListener(commandFuture -> {
            if(commandFuture.isSuccess()) {
                future().setSuccess(dcMeta);
            } else {
                future().setFailure(commandFuture.cause());
            }
        });
        sequenceCommandChain.execute(executors);

    }

    @Override
    protected void doReset() {
        // remove all clusters if failed
        dcMeta.getClusters().clear();
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    private  <T> Command<T> retry3TimesUntilSuccess(Command<T> command) {
        return factory.createRetryCommand(command);
    }

    protected ClusterMeta getOrCreateClusterMeta(ClusterTbl cluster) {
        return MapUtils.getOrCreate(dcMeta.getClusters(), cluster.getClusterName(), new ObjectFactory<ClusterMeta>(){
            @Override
            public ClusterMeta create() {
                ClusterMeta clusterMeta = new ClusterMeta(cluster.getClusterName());
                DcTbl proto = new DcTbl().setId(dcId);
                long activeDcId = clusterMetaService.getClusterMetaCurrentPrimaryDc(proto, cluster);
                clusterMeta.setActiveDc(dcNameMap.get(activeDcId));
                clusterMeta.setParent(dcMeta);
                clusterMeta.setBackupDcs(getBackupDcs(cluster, activeDcId));
                clusterMeta.setOrgId(Math.toIntExact(cluster.getClusterOrgId()));
                return clusterMeta;
            }
        });
    }

    private String getBackupDcs(ClusterTbl cluster, long activeDcId) {
        List<DcClusterTbl> relatedDcClusters = this.cluster2DcClusterMap.get(cluster.getId());
        StringBuilder sb = new StringBuilder();
        relatedDcClusters.forEach(dcClusterTbl -> {
            if(dcClusterTbl.getDcId() != activeDcId) {
                sb.append(dcNameMap.get(dcClusterTbl.getDcId())).append(",");
            }
        });
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public ShardMeta getOrCreateShardMeta(String clusterId, ShardTbl shard, long sentinelId) {
        ClusterMeta clusterMeta = dcMeta.findCluster(clusterId);
        if(clusterMeta == null) {
            throw new IllegalArgumentException(String.format("Cluster: %s not found in dcMeta", clusterId));
        }
        return MapUtils.getOrCreate(clusterMeta.getShards(), shard.getShardName(), new ObjectFactory<ShardMeta>() {
            @Override
            public ShardMeta create() {
                ShardMeta shardMeta = new ShardMeta(shard.getShardName());
                shardMeta.setParent(clusterMeta);
                shardMeta.setSentinelMonitorName(shard.getSetinelMonitorName());
                shardMeta.setSentinelId(sentinelId);
                return shardMeta;
            }
        });
    }

    class GetAllDcClusterShardDetailCommand extends AbstractCommand<Void> {

        private long dcId;

        public GetAllDcClusterShardDetailCommand(long dcId) {
            this.dcId = dcId;
        }

        @Override
        protected void doExecute() throws Exception {
            try {
                dcClusterShards = dcClusterShardService.findAllByDcId(dcId);
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            dcClusterShards = null;
        }

        @Override
        public String getName() {
            return GetAllDcClusterShardDetailCommand.class.getSimpleName();
        }
    }

    class Cluster2DcClusterMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                cluster2DcClusterMap = Maps.newHashMap();
                List<DcClusterTbl> allDcClusters = dcClusterService.findAllDcClusters();

                for (DcClusterTbl dcClusterTbl : allDcClusters) {
                    List<DcClusterTbl> dcClusters = MapUtils.getOrCreate(cluster2DcClusterMap,
                            dcClusterTbl.getClusterId(), new ObjectFactory<List<DcClusterTbl>>() {
                                @Override
                                public List<DcClusterTbl> create() {
                                    return new LinkedList<>();
                                }
                            });
                    dcClusters.add(dcClusterTbl);
                }
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            cluster2DcClusterMap = null;
        }

        @Override
        public String getName() {
            return Cluster2DcClusterMapCommand.class.getSimpleName();
        }
    }

    class GetDcIdNameMapCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                dcNameMap = dcService.dcNameMap();
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            dcNameMap = null;
        }

        @Override
        public String getName() {
            return GetDcIdNameMapCommand.class.getSimpleName();
        }
    }

    class BuildDcMetaCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            try {
                for (DcClusterShardTbl dcClusterShard : dcClusterShards) {
                    ClusterMeta clusterMeta = getOrCreateClusterMeta(dcClusterShard.getClusterInfo());

                    ShardMeta shardMeta = getOrCreateShardMeta(clusterMeta.getId(),
                            dcClusterShard.getShardInfo(), dcClusterShard.getSetinelId());

                    RedisTbl redis = dcClusterShard.getRedisInfo();
                    if (Server.SERVER_ROLE.KEEPER.sameRole(redis.getRedisRole())) {
                        shardMeta.addKeeper(redisMetaService.getKeeperMeta(shardMeta, redis));
                    } else {
                        shardMeta.addRedis(redisMetaService.getRedisMeta(shardMeta, redis));
                    }
                }
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return BuildDcMetaCommand.class.getSimpleName();
        }
    }


    /**------------------Visible for Test-----------------------*/
    public DcMetaBuilder setDcNameMap(Map<Long, String> dcNameMap) {
        this.dcNameMap = dcNameMap;
        return this;
    }

    public DcMetaBuilder setCluster2DcClusterMap(Map<Long, List<DcClusterTbl>> cluster2DcClusterMap) {
        this.cluster2DcClusterMap = cluster2DcClusterMap;
        return this;
    }

    public DcMetaBuilder setDcClusterShards(List<DcClusterShardTbl> dcClusterShards) {
        this.dcClusterShards = dcClusterShards;
        return this;
    }
}
