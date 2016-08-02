package com.ctrip.xpipe.redis.meta.server.rest;


import java.util.List;

import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface KeeperMetaApi extends MetaApi{
	
	/**
	 * merge all meta servers result
	 * @param host
	 * @param port
	 * @return
	 */
	
	List<KeeperTransMeta> getAllKeepersByKeeperContainer(String keeperContainerHost, int keeperContainerPort);

	/**
	 * get current meta server keepers
	 * @param keeperContainerHost
	 * @param keeperContainerPort
	 * @return
	 */
	List<KeeperTransMeta> getKeepersByKeeperContainer(String keeperContainerHost, int keeperContainerPort);
}
