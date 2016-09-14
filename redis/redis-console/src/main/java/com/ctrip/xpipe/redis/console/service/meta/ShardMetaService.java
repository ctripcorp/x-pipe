package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Map;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface ShardMetaService {

	ShardMeta loadShardMeta(ClusterMeta clusterMeta,ClusterTbl clusterTbl, ShardTbl shardTbl, DcMetaQueryVO dcMetaQueryVO);
	
	ShardMeta getShardMeta(String dcName, String clusterName, String shardName);
	
	ShardMeta encodeShardMeta(DcTbl dcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo, Map<Triple<Long,Long,Long>,RedisTbl> activekeepers);
}
