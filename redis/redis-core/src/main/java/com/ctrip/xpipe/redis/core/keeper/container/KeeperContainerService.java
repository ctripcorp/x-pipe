package com.ctrip.xpipe.redis.core.keeper.container;

import java.util.List;

import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface KeeperContainerService {
	
	void addKeeper(KeeperTransMeta keeperTransMeta);
	
	void addOrStartKeeper(KeeperTransMeta keeperTransMeta);
	
	void removeKeeper(KeeperTransMeta keeperTransMeta);

	void startKeeper(KeeperTransMeta keeperTransMeta);

	void stopKeeper(KeeperTransMeta keeperTransMeta);
	
	List<KeeperInstanceMeta> getAllKeepers();
}
