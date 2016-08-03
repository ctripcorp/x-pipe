package com.ctrip.xpipe.redis.core.metaserver;

import java.util.List;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerKeeperService extends MetaServerService{

	/***********************for keeper*******************/
	
	void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta);

	
	/***********************for container*******************/
	
	/**
	 * meta server merge all meta server's results
	 * @param keeperContainerMeta
	 * @return
	 */
	List<KeeperTransMeta> getAllKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta);
	

}
