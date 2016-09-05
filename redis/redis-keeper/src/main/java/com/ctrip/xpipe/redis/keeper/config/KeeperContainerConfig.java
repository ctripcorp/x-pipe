package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.redis.core.config.MetaServerAddressAware;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public interface KeeperContainerConfig extends MetaServerAddressAware{
	
    String getReplicationStoreDir();
}
