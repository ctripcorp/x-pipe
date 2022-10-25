package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;

import java.io.File;
import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public class GtidReplicationStoreManager extends DefaultReplicationStoreManager implements ReplicationStoreManager {

    private boolean openIndexing = false;

    private RedisOpParser redisOpParser;

    public GtidReplicationStoreManager(KeeperConfig keeperConfig, ClusterId clusterId, ShardId shardId, String keeperRunid,
                                       File baseDir, KeeperMonitor keeperMonitor, RedisOpParser redisOpParser) {
        super(keeperConfig, clusterId, shardId, keeperRunid, baseDir, keeperMonitor);
        this.redisOpParser = redisOpParser;
    }

    @Override
    protected ReplicationStore createReplicationStore(File storeBaseDir, KeeperConfig keeperConfig, String keeperRunid, KeeperMonitor keeperMonitor) throws IOException {
        if (openIndexing) {
            return new GtidReplicationStore(storeBaseDir, keeperConfig, keeperRunid, keeperMonitor, redisOpParser);
        } else {
            return new DefaultReplicationStore(storeBaseDir, keeperConfig, keeperRunid, keeperMonitor);
        }
    }

    @Override
    public void setOpenIndexing(boolean openIndexing) {
        this.openIndexing = openIndexing;
    }
}
