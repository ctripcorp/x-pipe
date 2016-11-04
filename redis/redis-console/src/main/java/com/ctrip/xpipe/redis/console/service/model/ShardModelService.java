package com.ctrip.xpipe.redis.console.service.model;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.ShardModel;

public interface ShardModelService {
	List<ShardModel> getAllShardModel(String dcName, String clusterName);
	ShardModel getShardModel(String dcName, String clusterName, String shardName);
}
