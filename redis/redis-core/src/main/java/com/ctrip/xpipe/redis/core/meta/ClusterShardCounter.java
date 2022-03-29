package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.core.IVisitor;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.utils.MapUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 21, 2017
 */
public class ClusterShardCounter implements IVisitor{

    private Map<String, Set<String>> clusters = new ConcurrentHashMap<>();


    public int getClusterCount() {
        return clusters.size();
    }

    public int getShardCount() {

        AtomicInteger count = new AtomicInteger();
        clusters.forEach((clusterName, shards) -> count.addAndGet(shards.size()));
        return count.get();
    }

    public Map<String, Set<String>> getClusters() {
        return clusters;
    }

    @Override
    public void visitCluster(ClusterMeta cluster) {

        getOrCreate(cluster);
        cluster.getShards().forEach((shardName, shardMeta) -> shardMeta.accept(this));

    }

    private Set<String> getOrCreate(ClusterMeta cluster) {

        return MapUtils.getOrCreate(clusters, cluster.getId(), new ObjectFactory<Set<String>>() {

            @Override
            public Set<String> create() {
                return new HashSet<>();
            }
        });
    }

    @Override
    public void visitDc(DcMeta dc) {
        dc.getClusters().forEach((clusterName, clusterMeta) -> clusterMeta.accept(this));

    }

    @Override
    public void visitKeeper(KeeperMeta keeper) {

    }

    @Override
    public void visitKeeperContainer(KeeperContainerMeta keeperContainer) {

    }

    @Override
    public void visitMetaServer(MetaServerMeta metaServer) {

    }

    @Override
    public void visitRedis(RedisMeta redis) {

    }

    @Override
    public void visitRoute(RouteMeta route) {

    }

    @Override
    public void visitSentinel(SentinelMeta sentinel) {

    }

    @Override
    public void visitAz(AzMeta az) {

    }

    @Override
    public void visitShard(ShardMeta shard) {

        Set<String> shards = getOrCreate(shard.parent());
        shards.add(shard.getId());
    }

    @Override
    public void visitXpipe(XpipeMeta xpipe) {

        xpipe.getDcs().forEach((dcName, dcMeta) -> dcMeta.accept(this));

    }

    @Override
    public void visitZkServer(ZkServerMeta zkServer) {

    }

    @Override
    public void visitApplier(ApplierMeta applier) {

    }

    @Override
    public void visitApplierContainer(ApplierContainerMeta applierContainer) {

    }

    @Override
    public void visitSource(SourceMeta source) {

    }
}
