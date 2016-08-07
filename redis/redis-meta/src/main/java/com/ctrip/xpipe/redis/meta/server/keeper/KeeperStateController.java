package com.ctrip.xpipe.redis.meta.server.keeper;


import java.net.InetSocketAddress;
import java.util.List;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public interface KeeperStateController {
	
	void addKeeper(KeeperTransMeta keeperTransMeta);
	
	void removeKeeper(KeeperTransMeta keeperTransMeta);

	/**
	 * @param keeperMeta
	 * @param redisMasterAddress when dc is backup, this address should be another keeper
	 */
	void makeKeeperActive(KeeperMeta keeperMeta, InetSocketAddress redisMasterAddress);

	void makeKeeperBackup(KeeperMeta keeperMeta, KeeperMeta activeKeeper);

	/**
	 * @param keeperMetas
	 * @param redisMasterAddress  when dc is backup, this address should be another keeper
	 */
	void makeSureKeeperStateRight(List<KeeperMeta> keeperMetas, InetSocketAddress redisMasterAddress);

}
