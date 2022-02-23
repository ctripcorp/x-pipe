package com.ctrip.xpipe.redis.console.dal;

import com.ctrip.xpipe.redis.core.IVisitor;
import com.ctrip.xpipe.redis.core.entity.*;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Nov 10, 2017
 */
public class XPipeMetaVisitor implements IVisitor{

    XpipeMeta xPipeMeta;

    Map<String, DcMeta> dcMetaMap = new HashMap<>();
    Map<String, ClusterMeta> clusterMetaMap = new HashMap<>();
    Map<String, ShardMeta> shardMetaMap = new HashMap<>();
    Map<String, RedisMeta> redisMetaMap = new HashMap<>();
    Map<Long, KeeperContainerMeta> kcMap = new HashMap<>();
    Map<String, KeeperMeta> keeperMetaMap = new HashMap<>();

    public XPipeMetaVisitor(XpipeMeta xpipeMeta) {
        this.xPipeMeta = xpipeMeta;
        getDCs(xpipeMeta);
    }

    private void getDCs(XpipeMeta xpipeMeta) {
        dcMetaMap = new HashMap<>(xpipeMeta.getDcs());
        dcMetaMap.values().forEach(dcMeta -> {
            getKCs(dcMeta);
            getClusters(dcMeta);
        });
    }

    private void getKCs(DcMeta dcMeta) {
        dcMeta.getKeeperContainers().forEach(kc -> kcMap.put(kc.getId(), kc));
    }

    private void getClusters(DcMeta dcMeta) {
        clusterMetaMap.putAll(dcMeta.getClusters());
        dcMeta.getClusters().forEach((id, cluster) -> getShard(cluster));
    }

    private void getShard(ClusterMeta cluster) {
        shardMetaMap.putAll(cluster.getShards());
        cluster.getShards().values().forEach(shard -> {
            getRedis(shard);
            getKeeper(shard);
        });
    }

    private void getRedis(ShardMeta shard) {
        shard.getRedises().forEach(redis -> redisMetaMap.put(redis.getId(), redis));
    }

    private void getKeeper(ShardMeta shard) {
        shard.getKeepers().forEach(keeper -> keeperMetaMap.put(keeper.getId(), keeper));
    }

    @Override
    public void visitAz(AzMeta az) {

    }

    @Override
    public void visitCluster(ClusterMeta cluster) {
        ClusterMeta clusterMeta = clusterMetaMap.get(cluster.getId());
        Assert.assertTrue(clusterMeta.equals(cluster));
        cluster.getShards().values().forEach(shard -> shard.accept(this));
    }

    @Override
    public void visitDc(DcMeta dc) {
        DcMeta localStoredDC = this.dcMetaMap.get(dc.getId());
        Assert.assertTrue(localStoredDC.equals(dc));
        dc.getKeeperContainers().forEach(kc -> kc.accept(this));
        dc.getClusters().values().forEach(cluster -> cluster.accept(this));
    }

    @Override
    public void visitKeeper(KeeperMeta keeper) {
        KeeperMeta keeperMeta = keeperMetaMap.get(keeper.getId());
        Assert.assertTrue(keeperMeta.equals(keeper));
    }

    @Override
    public void visitKeeperContainer(KeeperContainerMeta keeperContainer) {
        KeeperContainerMeta kc = kcMap.get(keeperContainer.getId());
        Assert.assertTrue(kc.equals(keeperContainer));
    }

    @Override
    public void visitMetaServer(MetaServerMeta metaServer) {

    }

    @Override
    public void visitRedis(RedisMeta redis) {
        RedisMeta redisMeta = redisMetaMap.get(redis.getId());
        Assert.assertTrue(redisMeta.equals(redis));
    }

    @Override
    public void visitRedisConfigCheckRule(RedisConfigCheckRuleMeta redisConfigCheckRule) {

    }

    @Override
    public void visitRoute(RouteMeta route) {

    }

    @Override
    public void visitSentinel(SentinelMeta sentinel) {

    }

    @Override
    public void visitShard(ShardMeta shard) {
        ShardMeta shardMeta = shardMetaMap.get(shard.getId());
        Assert.assertTrue(shardMeta.equals(shard));
        shard.getKeepers().forEach(keeper -> keeper.accept(this));
        shard.getRedises().forEach(redis -> redis.accept(this));
    }

    @Override
    public void visitXpipe(XpipeMeta xpipe) {
        Assert.assertEquals(xpipe.getDcs(), this.xPipeMeta.getDcs());
        xpipe.getDcs().values().forEach(dcMeta -> dcMeta.accept(this));
    }

    @Override
    public void visitZkServer(ZkServerMeta zkServer) {

    }
}
