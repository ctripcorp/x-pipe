package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.store.GtidReplicationStoreManager;

import java.io.File;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public class GtidRedisKeeperServer extends DefaultRedisKeeperServer implements RedisKeeperServer {

    private RedisOpParser redisOpParser;

    public GtidRedisKeeperServer(KeeperMeta currentKeeperMeta, KeeperConfig keeperConfig, File baseDir,
                                 LeaderElectorManager leaderElectorManager, KeepersMonitorManager keepersMonitorManager,
                                 KeeperResourceManager resourceManager, RedisOpParser redisOpParser) {
        super(currentKeeperMeta, keeperConfig, baseDir, leaderElectorManager, keepersMonitorManager, resourceManager);
        this.redisOpParser = redisOpParser;
    }

    @Override
    protected ReplicationStoreManager createReplicationStoreManager(KeeperConfig keeperConfig,
                                                                    ClusterId clusterId, ShardId shardId,
                                                                    KeeperMeta currentKeeperMeta, File baseDir,
                                                                    KeeperMonitor keeperMonitor) {
        return new GtidReplicationStoreManager(keeperConfig, clusterId, shardId, currentKeeperMeta.getId(),
                baseDir, keeperMonitor, redisOpParser);
    }

    @Override
    public KeeperTransMeta.KeeperReplType getKeeperReplType() {
        return KeeperTransMeta.KeeperReplType.REPL_HYTERO;
    }

    @Override
    public KeeperInstanceMeta getKeeperInstanceMeta() {
        KeeperInstanceMeta meta = super.getKeeperInstanceMeta();
        meta.setKeeperReplType(getKeeperReplType());
        return meta;
    }
}
