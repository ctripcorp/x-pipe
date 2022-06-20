package com.ctrip.xpipe.redis.console.service.model;

import com.ctrip.xpipe.redis.console.model.ReplDirectionInfoModel;
import com.ctrip.xpipe.redis.console.model.ShardModel;

import java.util.List;

public interface ShardModelService {

	List<ShardModel> getAllShardModel(String dcName, String clusterName);

	ShardModel getShardModel(String dcName, String clusterName, String shardName,
							 boolean isSourceShard, ReplDirectionInfoModel replDirectionInfoModel);

	ShardModel getSourceShardModel(String clusterName, String srcDcName, String toDcName, String shardName);

}
