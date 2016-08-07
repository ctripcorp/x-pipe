package com.ctrip.xpipe.redis.meta.server.meta;


import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface MetaServerMetaUpdateOperation {
	
	/**********update from console***********/
	void refresh(DcMeta dcMeta);
	
	void refresh(ClusterMeta clusterMeta);
}
