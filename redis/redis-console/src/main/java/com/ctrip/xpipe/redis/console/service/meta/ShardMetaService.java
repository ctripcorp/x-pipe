package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface ShardMetaService {

	ShardMeta loadShardMeta(ClusterMeta clusterMeta,ClusterTbl clusterTbl, ShardTbl shardTbl, DcMetaQueryVO dcMetaQueryVO);
	
	ShardMeta getShardMeta(String dcName, String clusterName, String shardName);
	
	ShardMeta getShardMeta(DcTbl dcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo);
}
