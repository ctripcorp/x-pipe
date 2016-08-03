package com.ctrip.xpipe.redis.core.metaserver;

import java.util.List;

import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerService {
	
	
	List<MetaServerMeta> getAllMetaServers();

}
