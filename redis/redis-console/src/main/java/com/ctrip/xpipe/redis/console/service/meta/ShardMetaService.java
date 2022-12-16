package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

import java.util.Map;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface ShardMetaService {

	ShardMeta loadShardMeta(ClusterMeta clusterMeta,ClusterTbl clusterTbl, ShardTbl shardTbl, DcMetaQueryVO dcMetaQueryVO);
	
	ShardMeta getShardMeta(String dcName, String clusterName, String shardName, Map<Long, Long> keeperContainerId2DcMap);
	
	ShardMeta getShardMeta(DcTbl dcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo, Map<Long, Long> keeperContainerId2DcMap);

	ShardMeta getSourceShardMeta(DcTbl srcDcInfo, DcTbl currentDcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo, DcClusterTbl dcClusterTbl,
								 Map<Long, Long> keeperContainerId2DcMap, long replId);
}
