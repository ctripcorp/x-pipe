package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface ClusterMetaService {
	
	ClusterMeta getClusterMeta(String dcName, String clusterName);

	ClusterMeta loadClusterMeta(DcMeta dcMeta, ClusterTbl clusterTbl, DcMetaQueryVO dcMetaQueryVO);

	long getClusterMetaCurrentPrimaryDc(DcTbl dcInfo, ClusterTbl clusterInfo);
}
