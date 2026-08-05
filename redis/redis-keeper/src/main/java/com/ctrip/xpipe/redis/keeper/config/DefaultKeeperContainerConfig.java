package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.api.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultKeeperContainerConfig implements KeeperContainerConfig {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerConfig.class);

    public static final String REPLICATION_STORE_DIR = "replication.store.dir";
    public static final String KEY_MODE = "mode";
    public static final String DISK_CHECK_INTERVAL_MILL = "disk.check.interval.mill";
    public static final String CHECK_ROUND = "health.check.round.before.mark.down";
    public static final String ELECTION_RESET_INTERVAL = "election.reset.interval.min";
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
    public String getMode() {
        String raw = config.get(KEY_MODE, MODE_NORMAL);
        if (raw == null || raw.trim().isEmpty()) {
            return MODE_NORMAL;
        }
        String mode = raw.trim();
        if (MODE_NORMAL.equalsIgnoreCase(mode)) {
            return MODE_NORMAL;
        }
        if (MODE_TFS.equalsIgnoreCase(mode)) {
            return MODE_TFS;
        }
        logger.error("[getMode] invalid mode={}, fallback to {}", mode, MODE_NORMAL);
        return MODE_NORMAL;
    }

    @Override
    public boolean isTfsMode() {
        return MODE_TFS.equals(getMode());
    }

    @Override
    public int diskCheckInterval() {
        return Integer.parseInt(config.get(DISK_CHECK_INTERVAL_MILL, "30000"));
    }

    @Override
    public int checkRoundBeforeMarkDown() {
        return Integer.parseInt(config.get(CHECK_ROUND, "3"));
    }

    @Override
    public int keeperLeaderResetMinInterval() {
        return Integer.parseInt(config.get(ELECTION_RESET_INTERVAL, "600"));
    }

    private String getDefaultRdsDir() {
        return System.getProperty("user.dir");
    }
}
