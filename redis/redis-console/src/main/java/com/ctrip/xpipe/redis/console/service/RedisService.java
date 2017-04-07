package com.ctrip.xpipe.redis.console.service;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import org.unidal.dal.jdbc.DalException;

public interface RedisService {
	
	RedisTbl find(long id);
	List<RedisTbl> findAllByDcClusterShard(long dcClusterShardId);
	List<RedisTbl> findAllByDcClusterShard(String dcId, String clusterId, String shardId);
	void insert(RedisTbl ... redises) throws DalException;
	void delete(RedisTbl ... redises);
	void updateByPK(RedisTbl redis);
	void batchUpdate(List<RedisTbl> redises);
	void updateRedises(String dcName, String clusterName, String shardName, ShardModel shardModel);
	
}
