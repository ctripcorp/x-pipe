package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.api.config.Config;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class DefaultKeeperContainerConfig implements KeeperContainerConfig {
    public static final String REPLICATION_STORE_DIR = "replication.store.dir";
    public static final String META_SERVER_URL = "meta.server.url";
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
    public String getMetaServerUrl() {
        return config.get(META_SERVER_URL, "http://127.0.0.1:9747");
    }

    private String getDefaultRdsDir() {
        return System.getProperty("user.dir");
    }
}
