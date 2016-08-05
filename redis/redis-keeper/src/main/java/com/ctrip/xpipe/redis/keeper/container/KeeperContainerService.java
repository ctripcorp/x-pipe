package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperBadRequestException;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.meta.MetaService;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.File;
import java.util.Map;
import java.util.Set;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class KeeperContainerService {
    @Autowired
    private LeaderElectorManager leaderElectorManager;
    @Autowired
    private MetaService metaService;
    @Autowired
    private KeeperContainerConfig keeperContainerConfig;

    private Set<Integer> runningPorts = Sets.newConcurrentHashSet();
    private Map<String, RedisKeeperServer> redisKeeperServers = Maps.newConcurrentMap();

    public RedisKeeperServer add(KeeperTransMeta keeperTransMeta) {
        KeeperMeta keeperMeta = keeperTransMeta.getKeeperMeta();
        enrichKeeperMetaFromKeeperTransMeta(keeperMeta, keeperTransMeta);

        String keeperServerKey = assembleKeeperServerKey(keeperTransMeta);
        if (!redisKeeperServers.containsKey(keeperServerKey)) {
            synchronized (this) {
                if (!redisKeeperServers.containsKey(keeperServerKey)) {
                    if (runningPorts.contains(keeperMeta.getPort())) {
                        throw new RedisKeeperBadRequestException(
                                String.format("Add keeper for cluster %s shard %s failed since port %d is already used",
                                        keeperTransMeta.getClusterId(), keeperTransMeta.getShardId(), keeperMeta
                                                .getPort()));
                    }
                    try {
                        RedisKeeperServer redisKeeperServer = doAdd(keeperTransMeta, keeperMeta);
                        redisKeeperServers.put(keeperServerKey, redisKeeperServer);
                        runningPorts.add(keeperMeta.getPort());
                        return redisKeeperServer;
                    } catch (Throwable ex) {
                        throw new RedisKeeperRuntimeException(String.format("Add keeper for cluster %s shard %s failed",
                                keeperTransMeta.getClusterId(), keeperTransMeta.getShardId()), ex);
                    }
                }
            }
        }

        throw new RedisKeeperBadRequestException(String.format("Keeper already exists for cluster %s shard %s",
                keeperTransMeta.getClusterId(), keeperTransMeta.getShardId()));
    }

    private RedisKeeperServer doAdd(KeeperTransMeta keeperTransMeta, KeeperMeta keeperMeta) throws Exception {
        ReplicationStoreManager replicationStoreManager = createReplicationStoreManager(keeperTransMeta);

        return createRedisKeeperServer(keeperMeta, replicationStoreManager, metaService);
    }

    private void enrichKeeperMetaFromKeeperTransMeta(KeeperMeta keeperMeta, KeeperTransMeta keeperTransMeta) {
        ClusterMeta clusterMeta = new ClusterMeta(keeperTransMeta.getClusterId());
        ShardMeta shardMeta = new ShardMeta(keeperTransMeta.getShardId());
        shardMeta.setParent(clusterMeta);
        keeperMeta.setParent(shardMeta);
    }

    private RedisKeeperServer createRedisKeeperServer(KeeperMeta keeper,
                                                      ReplicationStoreManager replicationStoreManager,
                                                      MetaService metaService) throws Exception {

        RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(keeper,
                replicationStoreManager, metaService, leaderElectorManager);

        register(redisKeeperServer);
        return redisKeeperServer;
    }

    private ReplicationStoreManager createReplicationStoreManager(KeeperTransMeta keeperTransMeta) {
        File storeDir = getReplicationStoreDir(keeperTransMeta.getKeeperMeta());
        ReplicationStoreManager replicationStoreManager = new DefaultReplicationStoreManager(
                keeperTransMeta.getClusterId(), keeperTransMeta.getShardId(), storeDir);
        return replicationStoreManager;
    }

    private void register(RedisKeeperServer redisKeeperServer) throws Exception {
        ComponentRegistryHolder.getComponentRegistry().add(redisKeeperServer);
    }

    private File getReplicationStoreDir(KeeperMeta keeperMeta) {
        String baseDir = keeperContainerConfig.getReplicationStoreDir();
        baseDir = StringUtils.trimTrailingCharacter(baseDir, '/');
        return new File(String.format("%s/replication_store_%s", baseDir, keeperMeta.getPort()));
    }

    private String assembleKeeperServerKey(KeeperTransMeta keeperTransMeta) {
        return String.format("%s-%s", keeperTransMeta.getClusterId(), keeperTransMeta.getShardId());
    }
}
