package com.ctrip.xpipe.redis.meta.server.rest;

import java.util.Map;

import com.ctrip.xpipe.redis.core.entity.KeeperKey;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

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
	Map<KeeperKey, KeeperMeta>  getAllKeepersByKeeperContainer(String keeperContainerHost, int keeperContainerPort);

	/**
	 * get current meta server keepers
	 * @param keeperContainerHost
	 * @param keeperContainerPort
	 * @return
	 */
	Map<KeeperKey, KeeperMeta>  getKeepersByKeeperContainer(String keeperContainerHost, int keeperContainerPort);

}
