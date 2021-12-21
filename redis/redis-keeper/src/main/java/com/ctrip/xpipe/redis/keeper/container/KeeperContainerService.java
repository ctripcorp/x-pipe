package com.ctrip.xpipe.redis.keeper.container;


import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class KeeperContainerService {
	
	@Autowired
	KeeperConfig keeperConfig;
    @Autowired
    private LeaderElectorManager leaderElectorManager;
    @Autowired
    private KeeperContainerConfig keeperContainerConfig;
    @Autowired
    private KeepersMonitorManager keepersMonitorManager;
    @Autowired
    private KeeperResourceManager resourceManager;

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
                        throw new RedisKeeperRuntimeException(
                                new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_EXIST,
                                        String.format("Add keeper for cluster %d shard %d failed since port %d is already used",
                                        keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId(), keeperMeta.getPort())),
                                null);
                    }
                    try {
                        RedisKeeperServer redisKeeperServer = doAdd(keeperTransMeta, keeperMeta);
                        cacheKeeper(keeperServerKey, redisKeeperServer);
                        return redisKeeperServer;
                    } catch (Throwable ex) {
                        throw new RedisKeeperRuntimeException(
                                new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                                        String.format("Add keeper for cluster %d shard %d failed",
                                                keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId())), ex);
                    }
                }
            }
        }

        throw new RedisKeeperRuntimeException(
                new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_EXIST,
                        String.format("Keeper already exists for cluster %d shard %d",
                                keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId())), null);
    }

    public RedisKeeperServer addOrStart(KeeperTransMeta keeperTransMeta) {
        String keeperServerKey = assembleKeeperServerKey(keeperTransMeta);
        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);
        if (keeperServer == null) {
            return add(keeperTransMeta);
        }

        start(ClusterId.from(keeperTransMeta.getClusterDbId()), ShardId.from(keeperTransMeta.getShardDbId()));

        return keeperServer;
    }

    public List<RedisKeeperServer> list() {
        return Lists.newArrayList(redisKeeperServers.values());
    }

    public void start(ClusterId clusterId, ShardId shardId) {
        String keeperServerKey = assembleKeeperServerKey(clusterId, shardId);

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Start keeper for cluster %s shard %s failed since keeper doesn't exist",
                                    clusterId, shardId)), null);
        }

        if (keeperServer.getLifecycleState().isStarted()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_STARTED,
                            String.format("Keeper for cluster %s shard %s already started",
                                    clusterId, shardId)), null);
        }

        try {
            keeperServer.start();
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Start keeper failed for cluster %s shard %s",
                                    clusterId, shardId)), ex);
        }
    }

    public void stop(ClusterId clusterId, ShardId shardId) {
        String keeperServerKey = assembleKeeperServerKey(clusterId, shardId);

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Stop keeper for cluster %s shard %s failed since keeper doesn't exist",
                                    clusterId, shardId)), null);
        }

        if (keeperServer.getLifecycleState().isStopped()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_STOPPED,
                            String.format("Keeper for cluster %s shard %s already stopped",
                                    clusterId, shardId)), null);
        }

        try {
            keeperServer.stop();
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Stop keeper failed for cluster %s shard %s",
                                    clusterId, shardId)), ex);
        }
    }

    public void remove(ClusterId clusterId, ShardId shardId) {
        String keeperServerKey = assembleKeeperServerKey(clusterId, shardId);

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            return;
        }

        try {
            // 1. stop and dispose keeper server
            deRegister(keeperServer);

            // 2. remove keeper
            removeKeeperCache(keeperServerKey);

            // 3. clean external resources, such as replication stores
            keeperServer.destroy();
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Remove keeper failed for cluster %s shard %s",
                                    clusterId, shardId)), ex);
        }
    }

    private void cacheKeeper(String keeperServerKey, RedisKeeperServer redisKeeperServer) {
        redisKeeperServers.put(keeperServerKey, redisKeeperServer);
        runningPorts.add(redisKeeperServer.getListeningPort());
    }

    private void removeKeeperCache(String keeperServerKey) {
        RedisKeeperServer redisKeeperServer = redisKeeperServers.remove(keeperServerKey);
        if (redisKeeperServer != null) {
            runningPorts.remove(redisKeeperServer.getListeningPort());
        }
    }

    private RedisKeeperServer doAdd(KeeperTransMeta keeperTransMeta, KeeperMeta keeperMeta) throws Exception {

        File baseDir = getReplicationStoreDir(keeperMeta);

        return createRedisKeeperServer(keeperMeta, baseDir);
    }

    private void enrichKeeperMetaFromKeeperTransMeta(KeeperMeta keeperMeta, KeeperTransMeta keeperTransMeta) {
        ClusterMeta clusterMeta = new ClusterMeta().setDbId(keeperTransMeta.getClusterDbId());
        clusterMeta.setDbId(keeperTransMeta.getClusterDbId());
        ShardMeta shardMeta = new ShardMeta().setDbId(keeperTransMeta.getShardDbId());
        shardMeta.setDbId(keeperTransMeta.getShardDbId());
        shardMeta.setParent(clusterMeta);
        keeperMeta.setParent(shardMeta);
    }

    private RedisKeeperServer createRedisKeeperServer(KeeperMeta keeper,
                                                      File baseDir) throws Exception {

        RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(keeper, keeperConfig,
                baseDir, leaderElectorManager, keepersMonitorManager, resourceManager);

        register(redisKeeperServer);
        return redisKeeperServer;
    }

    private void register(RedisKeeperServer redisKeeperServer) throws Exception {
        ComponentRegistryHolder.getComponentRegistry().add(redisKeeperServer);
    }

    private void deRegister(RedisKeeperServer redisKeeperServer) throws Exception {
        ComponentRegistryHolder.getComponentRegistry().remove(redisKeeperServer);
    }

    private File getReplicationStoreDir(KeeperMeta keeperMeta) {
        String baseDir = keeperContainerConfig.getReplicationStoreDir();
        baseDir = StringUtils.trimTrailingCharacter(baseDir, '/');
        return new File(String.format("%s/replication_store_%s", baseDir, keeperMeta.getPort()));
    }

    private String assembleKeeperServerKey(KeeperTransMeta keeperTransMeta) {
        return assembleKeeperServerKey(ClusterId.from(keeperTransMeta.getClusterDbId()), ShardId.from(keeperTransMeta.getShardDbId()));
    }

    private String assembleKeeperServerKey(ClusterId clusterId, ShardId shardId) {
        return String.format("%s-%s", clusterId, shardId);
    }
}
