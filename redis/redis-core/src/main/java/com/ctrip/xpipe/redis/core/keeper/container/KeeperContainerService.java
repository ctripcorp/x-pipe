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

	/** 返回当前 KeeperContainer 的实际访问 IP；容器形态会实时按 hostname 反查。 */
	String resolveAddress();

	KeeperInstanceMeta infoPort(int port);

	void addKeeper(KeeperTransMeta keeperTransMeta);
	
	void addOrStartKeeper(KeeperTransMeta keeperTransMeta);
	
	void removeKeeper(KeeperTransMeta keeperTransMeta);

	/**
	 * Like {@link #removeKeeper(KeeperTransMeta)}, but never deletes the replication store — for a
	 * shared TFS store that must outlive this keeper instance.
	 */
	void releaseKeeper(KeeperTransMeta keeperTransMeta);

	void startKeeper(KeeperTransMeta keeperTransMeta);

	void stopKeeper(KeeperTransMeta keeperTransMeta);
	
	List<KeeperInstanceMeta> getAllKeepers();
}
