package com.ctrip.xpipe.redis.keeper.config;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public interface KeeperContainerConfig {

    String MODE_NORMAL = "NORMAL";

    String MODE_TFS = "TFS";
	
    String getReplicationStoreDir();

    /**
     * Process-level store path mode: {@link #MODE_NORMAL} or {@link #MODE_TFS}.
     */
    String getMode();

    boolean isTfsMode();

    int diskCheckInterval();

    int checkRoundBeforeMarkDown();

    int keeperLeaderResetMinInterval();

}
