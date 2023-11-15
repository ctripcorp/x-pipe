package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.api.config.Config;

import javax.annotation.PostConstruct;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultKeeperContainerConfig implements KeeperContainerConfig {
    public static final String REPLICATION_STORE_DIR = "replication.store.dir";
    public static final String DISK_CHECK_INTERVAL_MILL = "disk.check.interval.mill";
    private Config config;

    @PostConstruct
    private void init() {
        config = Config.DEFAULT;
    }

    @Override
    public String getReplicationStoreDir() {
        return config.get(REPLICATION_STORE_DIR, getDefaultRdsDir());
    }

    @Override
    public int diskCheckInterval() {
        return Integer.parseInt(config.get(DISK_CHECK_INTERVAL_MILL, "30000"));
    }

    private String getDefaultRdsDir() {
        return System.getProperty("user.dir");
    }
}
