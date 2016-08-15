package com.ctrip.xpipe.redis.console.service.meta;

import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
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

	public ShardMeta loadShardMeta(ClusterMeta clusterMeta,ClusterTbl clusterTbl, ShardTbl shardTbl, DcMetaQueryVO dcMetaQueryVO);
	
	public ShardMeta getShardMeta(String dcName, String clusterName, String shardName);
	
	public ShardMeta encodeShardMeta(DcTbl dcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo, Map<Triple<Long,Long,Long>,RedisTbl> activekeepers);
}
