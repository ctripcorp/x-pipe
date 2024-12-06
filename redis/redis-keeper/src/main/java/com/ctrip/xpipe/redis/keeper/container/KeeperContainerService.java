package com.ctrip.xpipe.redis.keeper.container;


import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.health.DiskHealthChecker;
import com.ctrip.xpipe.redis.keeper.health.HealthState;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class KeeperContainerService extends AbstractLifecycle implements TopElement, Observer {
	
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
    @Autowired
    private ContainerResourceManager containerResourceManager;
    @Autowired
    private GeneralRedisOpParser redisOpParser;
    @Autowired
    private DiskHealthChecker diskHealthChecker;
    @Autowired
    private SyncRateManager syncRateManager;

    private Map<String, RedisKeeperServer> redisKeeperServers = Maps.newConcurrentMap();

    private Logger logger = LoggerFactory.getLogger(KeeperContainerService.class);

    @Override
    protected void doInitialize() throws Exception {
        this.diskHealthChecker.addObserver(this);
    }

    @Override
    public void update(Object args, Observable observable) {
        if (args instanceof HealthState && !((HealthState) args).isUp()) {
            long currentTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            // reset leader
            for (RedisKeeperServer keeper: redisKeeperServers.values()) {
                if (keeper.isLeader()) {
                    long lastResetTime = keeper.getLastElectionResetTime();
                    if (lastResetTime <= currentTime
                            && currentTime - lastResetTime < keeperContainerConfig.keeperLeaderResetMinInterval()) {
                        logger.debug("[container_unhealthy][{}] last reset at: {}, skip", keeper.getReplId(), lastResetTime);
                        continue;
                    }

                    logger.info("[container_unhealthy][{}] reset leader", keeper.getReplId());
                    keeper.resetElection();
                }
            }
        }
    }

    public RedisKeeperServer add(KeeperTransMeta keeperTransMeta) {
        KeeperMeta keeperMeta = keeperTransMeta.getKeeperMeta();
        enrichKeeperMetaFromKeeperTransMeta(keeperMeta, keeperTransMeta);

        String keeperServerKey = assembleKeeperServerKey(keeperTransMeta);
        if (!redisKeeperServers.containsKey(keeperServerKey)) {
            synchronized (this) {
                if (!redisKeeperServers.containsKey(keeperServerKey)) {
                    if (!containerResourceManager.applyPort(keeperMeta.getPort())) {
                        throw new RedisKeeperRuntimeException(
                                new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_EXIST,
                                        String.format("Add keeper for cluster %d shard %d failed since port %d is already used",
                                                keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId(), keeperMeta.getPort())),
                                null);
                    }

                    File baseDir = getReplicationStoreDir(keeperMeta);
                    RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(keeperTransMeta.getReplId(), keeperMeta,
                            keeperConfig, baseDir, leaderElectorManager, keepersMonitorManager, resourceManager, syncRateManager, redisOpParser);

                    try {
                        register(redisKeeperServer);
                        redisKeeperServers.put(keeperServerKey, redisKeeperServer);
                        return redisKeeperServer;
                    } catch (Throwable regTh) {
                        logger.info("[add] register fail", regTh);
                        try {
                            deRegister(redisKeeperServer);
                        } catch (Throwable deregTh) {
                            logger.info("[add][rollback] deregister fail", deregTh);
                        }
                        if (!redisKeeperServer.getLifecycleState().isDisposed()) {
                            try {
                                LifecycleHelper.stopIfPossible(redisKeeperServer);
                                LifecycleHelper.disposeIfPossible(redisKeeperServer);
                            } catch (Throwable th) {
                                logger.info("[add][rollback] dispose fail", th);
                            }
                        }
                        if (!redisKeeperServers.containsKey(keeperServerKey)) {
                            containerResourceManager.releasePort(keeperMeta.getPort());
                        }

                        throw new RedisKeeperRuntimeException(
                                new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                                        String.format("Add keeper for cluster %d shard %d failed",
                                                keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId())), regTh);
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

        start(ReplId.from(keeperTransMeta.getReplId()));

        return keeperServer;
    }

    public List<RedisKeeperServer> list() {
        return Lists.newArrayList(redisKeeperServers.values());
    }

    public KeeperDiskInfo infoDisk() {
        return diskHealthChecker.getResult();
    }

    public void resetElection(ReplId replId) {
        String keeperServerKey = replId.toString();

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Reset election for %s failed since keeper doesn't exist", replId)), null);
        }

        if (!keeperServer.getLifecycleState().isStarted()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_STARTED,
                            String.format("Keeper for %s has not started", replId)), null);
        }

        keeperServer.resetElection();
    }

    public void releaseRdb(ReplId replId) throws IOException {
        String keeperServerKey = replId.toString();

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Release rdb for %s failed since keeper doesn't exist", replId)), null);
        }

        if (!keeperServer.getLifecycleState().isStarted()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_STARTED,
                            String.format("Keeper for %s has not started", replId)), null);
        }

        keeperServer.releaseRdb();
    }

    public void start(ReplId replId) {
        String keeperServerKey = replId.toString();

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Start keeper for %s failed since keeper doesn't exist", replId)), null);
        }

        if (keeperServer.getLifecycleState().isStarted()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_STARTED,
                            String.format("Keeper for %s already started", replId)), null);
        }

        try {
            keeperServer.start();
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Start keeper failed for %s", replId)), ex);
        }
    }

    public void stop(ReplId replId) {
        String keeperServerKey = replId.toString();

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Stop keeper for %s failed since keeper doesn't exist", replId)), null);
        }

        if (keeperServer.getLifecycleState().isStopped()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_STOPPED,
                            String.format("Keeper for %s already stopped", replId)), null);
        }

        try {
            keeperServer.stop();
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Stop keeper failed for %s", replId)), ex);
        }
    }

    public void remove(ReplId replId) {
        String keeperServerKey = replId.toString();

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
                            String.format("Remove keeper failed for %s", replId)), ex);
        }
    }

    private void removeKeeperCache(String keeperServerKey) {
        RedisKeeperServer redisKeeperServer = redisKeeperServers.remove(keeperServerKey);
        if (redisKeeperServer != null) {
            containerResourceManager.releasePort(redisKeeperServer.getListeningPort());
        }
    }

    private RedisKeeperServer doAdd(KeeperTransMeta keeperTransMeta, KeeperMeta keeperMeta) throws Exception {

        File baseDir = getReplicationStoreDir(keeperMeta);
        return createRedisKeeperServer(keeperTransMeta.getReplId(), keeperMeta, baseDir);
    }

    private void enrichKeeperMetaFromKeeperTransMeta(KeeperMeta keeperMeta, KeeperTransMeta keeperTransMeta) {
        ClusterMeta clusterMeta = new ClusterMeta().setDbId(keeperTransMeta.getClusterDbId());
        ShardMeta shardMeta = new ShardMeta().setDbId(keeperTransMeta.getShardDbId());
        shardMeta.setParent(clusterMeta);
        keeperMeta.setParent(shardMeta);
    }

    private RedisKeeperServer createRedisKeeperServer(Long replId, KeeperMeta keeper,
                                                      File baseDir) throws Exception {

        RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(replId, keeper, keeperConfig,
                baseDir, leaderElectorManager, keepersMonitorManager, resourceManager, syncRateManager, redisOpParser);

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
        return ReplId.from(keeperTransMeta.getReplId()).toString();
    }

    @VisibleForTesting
    protected void setRedisKeeperServers(Map<String, RedisKeeperServer> servers) {
        this.redisKeeperServers = servers;
    }

}
