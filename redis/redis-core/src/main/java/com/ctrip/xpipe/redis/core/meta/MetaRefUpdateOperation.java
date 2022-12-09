package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/1/4
 */
public interface MetaRefUpdateOperation extends ReadWriteSafe {

    default boolean updateKeeperActive(String dc, String clusterId, String shardId, KeeperMeta activeKeeper) { return write(()->doUpdateKeeperActive(dc, clusterId, shardId, activeKeeper)); }
    boolean doUpdateKeeperActive(String dc, String clusterId, String shardId, KeeperMeta activeKeeper);

    default boolean updateRedisMaster(String dc, String clusterId, String shardId, RedisMeta redisMaster) { return write(()->doUpdateRedisMaster(dc, clusterId, shardId, redisMaster)); }
    boolean doUpdateRedisMaster(String dc, String clusterId, String shardId, RedisMeta redisMaster);

    default void update(DcMeta dcMeta) { write(()->doUpdate(dcMeta)); }
    void doUpdate(DcMeta dcMeta);

    default void update(String dcId, ClusterMeta clusterMeta) { write(()->doUpdate(dcId, clusterMeta)); }
    void doUpdate(String dcId, ClusterMeta clusterMeta);

    default ClusterMeta removeCluster(String currentDc, String clusterId) { return write(()->doRemoveCluster(currentDc, clusterId)); }
    ClusterMeta doRemoveCluster(String currentDc, String clusterId);

    default void setSurviveKeepers(String dcId, String clusterId, String shardId, List<KeeperMeta> surviceKeepers) { write(()->doSetSurviveKeepers(dcId, clusterId, shardId, surviceKeepers)); }
    void doSetSurviveKeepers(String dcId, String clusterId, String shardId, List<KeeperMeta> surviceKeepers);

    default void setRedisGtidAndSids(String dcId, String clusterId, String shardId, RedisMeta redisMeta, String gtid, String sids) { write(()->doSetRedisGtidAndSids(dcId, clusterId, shardId, redisMeta, gtid, sids)); }
    void doSetRedisGtidAndSids(String dc, String clusterId, String shardId, RedisMeta redisMeta, String gtid, String sids);
}
