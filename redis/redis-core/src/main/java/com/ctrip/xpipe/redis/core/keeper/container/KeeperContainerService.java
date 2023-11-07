package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface KeeperContainerService {

	KeeperInstanceMeta infoPort(int port);

	void addKeeper(KeeperTransMeta keeperTransMeta);
	
	void addOrStartKeeper(KeeperTransMeta keeperTransMeta);
	
	void removeKeeper(KeeperTransMeta keeperTransMeta);

	void startKeeper(KeeperTransMeta keeperTransMeta);

	void stopKeeper(KeeperTransMeta keeperTransMeta);
	
	List<KeeperInstanceMeta> getAllKeepers();
}
