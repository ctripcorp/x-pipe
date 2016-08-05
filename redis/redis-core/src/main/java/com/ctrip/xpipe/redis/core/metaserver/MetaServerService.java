package com.ctrip.xpipe.redis.core.metaserver;

import java.util.List;

import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerService {
	
	public static final String HTTP_HEADER_FOWRARD = "forward";
	public static final String PATH_PREFIX = "/api/meta";
	
	public static final String PATH_GET_ALL_META_SERVERS = "/getallmetaservers";
	
	
	List<MetaServerMeta> getAllMetaServers();

}
