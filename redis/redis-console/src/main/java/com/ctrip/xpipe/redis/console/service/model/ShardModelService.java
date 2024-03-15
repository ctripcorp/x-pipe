package com.ctrip.xpipe.redis.console.service.model;

import com.ctrip.xpipe.redis.console.model.ReplDirectionInfoModel;
import com.ctrip.xpipe.redis.console.model.ShardModel;

import com.ctrip.xpipe.redis.console.model.ShardTbl;
import java.util.List;

public interface ShardModelService {

	List<ShardModel> getAllShardModel(String dcName, String clusterName);

	List<ShardModel> getMultiShardModel(String dcName, String clusterName, List<ShardTbl> shards,
		boolean isSourceShard, ReplDirectionInfoModel replDirectionInfoModel);

	ShardModel getShardModel(String dcName, String clusterName, String shardName,
		boolean isSourceShard, ReplDirectionInfoModel replDirectionInfoModel);

	ShardModel getSourceShardModel(String clusterName, String srcDcName, String toDcName, String shardName);

	boolean migrateShardKeepers(String dcName, String clusterName, ShardModel shardModel,
								String srcKeeperContainerIp, String targetKeeperContainerIp);

	boolean switchMaster(String dcName, String clusterName, ShardModel shardModel);

	void migrateAutoBalanceKeepers(String dcName, String clusterName, ShardModel shardModel,
								   String srcKeeperContainerIp, String targetKeeperContainerIp);
}
