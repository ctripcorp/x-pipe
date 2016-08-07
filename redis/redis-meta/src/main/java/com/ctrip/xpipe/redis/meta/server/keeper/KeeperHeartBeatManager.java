package com.ctrip.xpipe.redis.meta.server.keeper;


import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.keeper.HeartBeat;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public interface KeeperHeartBeatManager extends Observable, HeartBeat{
	
	void ping(KeeperInstanceMeta keeperInstanceMeta);
	
	boolean isKeeperAlive();
	
	void close();
	
}
