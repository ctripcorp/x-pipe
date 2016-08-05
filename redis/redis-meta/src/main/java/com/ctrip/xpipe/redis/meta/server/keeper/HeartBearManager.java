package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;

/**
 * @author wenchao.meng
 *
 * Aug 4, 2016
 */
public interface HeartBearManager {
	
	void heartBeat(KeeperInstanceMeta keeperInstanceMeta);

}
