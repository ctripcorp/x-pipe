package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.applier.DefaultApplierServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2022/6/9
 */
@Service
public class ApplierContainerService {

    @Autowired
    private LeaderElectorManager leaderElectorManager;

    @Autowired
    private ContainerResourceManager containerResourceManager;

    @Autowired
    private GeneralRedisOpParser redisOpParser;

    @Autowired
    private KeeperConfig keeperConfig;

    private Map<String, ApplierServer> applierServers = Maps.newConcurrentMap();

    private ScheduledExecutorService scheduled;

    private Logger logger = LoggerFactory.getLogger(ApplierContainerService.class);

    @PostConstruct
    public void postConstruct() {
        scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("ApplierChecker"));
        scheduled.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, ApplierServer> entry : applierServers.entrySet()) {
                ApplierServer applierServer = entry.getValue();
                ApplierServer.ApplierHealth health = applierServer.checkHealth();
                if (!health.isHealthy()) {
                    try {
                        logger.info("[applier unhealthy][{}] remove {}", health.getCause(), entry.getKey());
                        EventMonitor.DEFAULT.logEvent("ApplierUnhealthy", health.getCause());
                        remove(entry.getKey());
                    } catch (Throwable th) {
                        logger.info("[remove unhealthy applier][fail]", th);
                    }
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void preDestroy() {
        this.scheduled.shutdown();
    }



    public ApplierServer add(ApplierTransMeta applierTransMeta) {
        ApplierMeta applierMeta = applierTransMeta.getApplierMeta();

        String applierServerKey = assembleApplierServerKey(applierTransMeta);
        if (!applierServers.containsKey(applierServerKey)) {
            synchronized (this) {
                if (!applierServers.containsKey(applierServerKey)) {
                    if (!containerResourceManager.applyPort(applierMeta.getPort())) {
                        throw new RedisKeeperRuntimeException(
                                new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_EXIST,
                                        String.format("Add applier for cluster %d shard %d failed since port %d is already used",
                                                applierTransMeta.getClusterDbId(), applierTransMeta.getShardDbId(), applierMeta.getPort())),
                                null);
                    }

                    try {
                        ApplierServer applierServer = createApplierServer(applierTransMeta);
                        applierServers.put(applierServerKey, applierServer);
                        return applierServer;
                    } catch (Throwable ex) {
                        if (!applierServers.containsKey(applierServerKey)) {
                            containerResourceManager.releasePort(applierMeta.getPort());
                        }

                        throw new RedisKeeperRuntimeException(
                                new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                                        String.format("Add applier for cluster %d shard %d failed",
                                                applierTransMeta.getClusterDbId(), applierTransMeta.getShardDbId())), ex);
                    }
                }
            }
        }

        throw new RedisKeeperRuntimeException(
                new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_EXIST,
                        String.format("Applier already exists for cluster %d shard %d",
                                applierTransMeta.getClusterDbId(), applierTransMeta.getShardDbId())), null);
    }

    public void start(ClusterId clusterId, ShardId shardId) {
        String applierServerKey = assembleApplierServerKey(clusterId, shardId);

        ApplierServer applierServer = applierServers.get(applierServerKey);

        if (applierServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Start applier for cluster %s shard %s failed since applier doesn't exist",
                                    clusterId, shardId)), null);
        }

        if (applierServer.getLifecycleState().isStarted()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_STARTED,
                            String.format("Applier for cluster %s shard %s already started",
                                    clusterId, shardId)), null);
        }

        try {
            applierServer.start();
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Start keeper failed for cluster %s shard %s",
                                    clusterId, shardId)), ex);
        }
    }

    public void stop(ClusterId clusterId, ShardId shardId) {
        String applierServerKey = assembleApplierServerKey(clusterId, shardId);

        ApplierServer applierServer = applierServers.get(applierServerKey);

        if (applierServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Stop applier for cluster %s shard %s failed since applier doesn't exist",
                                    clusterId, shardId)), null);
        }

        if (applierServer.getLifecycleState().isStopped()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_STOPPED,
                            String.format("Applier for cluster %s shard %s already stopped",
                                    clusterId, shardId)), null);
        }

        try {
            applierServer.stop();
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Stop keeper failed for cluster %s shard %s",
                                    clusterId, shardId)), ex);
        }
    }

    public void remove(ClusterId clusterId, ShardId shardId) {
        try {
            String applierServerKey = assembleApplierServerKey(clusterId, shardId);
            remove(applierServerKey);
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Remove applier failed for cluster %s shard %s",
                                    clusterId, shardId)), ex);
        }
    }

    private void remove(String applierServerKey) throws Exception {
        ApplierServer applierServer = applierServers.get(applierServerKey);

        if (applierServer == null) {
            return;
        }

        deRegister(applierServer);

        ApplierServer removedApplierServer = applierServers.remove(applierServerKey);
        if (removedApplierServer != null) {
            containerResourceManager.releasePort(removedApplierServer.getListeningPort());
        }
    }

    public ApplierServer addOrStart(ApplierTransMeta applierTransMeta) {
        String applierServerKey = assembleApplierServerKey(applierTransMeta);
        ApplierServer applierServer = applierServers.get(applierServerKey);
        if (applierServer == null) {
            return add(applierTransMeta);
        }

        start(ClusterId.from(applierTransMeta.getClusterDbId()), ShardId.from(applierTransMeta.getShardDbId()));

        return applierServer;
    }

    public List<ApplierServer> list() {
        return Lists.newArrayList(applierServers.values());
    }

    private ApplierServer createApplierServer(ApplierTransMeta applierTransMeta) throws Exception {
        ApplierServer applierServer = new DefaultApplierServer(applierTransMeta.getClusterName(),
                ClusterId.from(applierTransMeta.getClusterDbId()), ShardId.from(applierTransMeta.getShardDbId()),
                applierTransMeta.getApplierMeta(), leaderElectorManager, redisOpParser, keeperConfig,
                applierTransMeta.getStateThreadNum(), applierTransMeta.getWorkerThreadNum(),
                applierTransMeta.getQpsThreshold(), applierTransMeta.getBytesPerSecondThreshold(),
                applierTransMeta.getMemoryThreshold(), applierTransMeta.getConcurrencyThreshold(),
                applierTransMeta.getSubenv());
        register(applierServer);

        return applierServer;
    }

    private void register(ApplierServer applierServer) throws Exception {
        ComponentRegistryHolder.getComponentRegistry().add(applierServer);
    }

    private void deRegister(ApplierServer applierServer) throws Exception {
        ComponentRegistryHolder.getComponentRegistry().remove(applierServer);
    }

    private String assembleApplierServerKey(ApplierTransMeta applierTransMeta) {
        return assembleApplierServerKey(ClusterId.from(applierTransMeta.getClusterDbId()), ShardId.from(applierTransMeta.getShardDbId()));
    }

    private String assembleApplierServerKey(ClusterId clusterId, ShardId shardId) {
        return String.format("%s-%s", clusterId, shardId);
    }

}
