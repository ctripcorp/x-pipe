package com.ctrip.xpipe.redis.keeper.config;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public interface KeeperContainerConfig {
	
    String getReplicationStoreDir();

    int diskCheckInterval();

    int checkRoundBeforeMarkDown();

    int keeperLeaderResetMinInterval();

}
