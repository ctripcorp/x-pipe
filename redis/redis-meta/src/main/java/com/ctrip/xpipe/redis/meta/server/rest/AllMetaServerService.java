package com.ctrip.xpipe.redis.meta.server.rest;

import java.util.List;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface AllMetaServerService extends MetaServerConsoleService, MetaServerKeeperService{
	
	/**
	 * current keepers 
	 * @param keeperContainerMeta
	 * @return
	 */
	List<KeeperTransMeta> getKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta);


}
