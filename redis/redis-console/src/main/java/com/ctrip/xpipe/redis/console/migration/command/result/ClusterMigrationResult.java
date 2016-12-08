package com.ctrip.xpipe.redis.console.migration.command.result;

import java.util.HashMap;
import java.util.Map;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class ClusterMigrationResult {
	private ClusterMigrationResultStatus status;
	private Map<String, ShardMigrationResult> shardMigrationResult = new HashMap<>();
	
	public ClusterMigrationResult() {
		status = ClusterMigrationResultStatus.PARTIAL_SUCCESS;
	}
	
	public ClusterMigrationResultStatus getResult() {
		return status;
	}
	
	public void setStatus(ClusterMigrationResultStatus status) {
		this.status = status;
	}
	
	public void putShardMigrationResult(String shardId, ShardMigrationResult result) {
		shardMigrationResult.put(shardId, result);
	}
	
	public ShardMigrationResult getResult(String shardId) {
		return shardMigrationResult.get(shardId);
	}
	
	public Map<String, ShardMigrationResult> getResults() {
		return shardMigrationResult;
	}
	
	public static enum ClusterMigrationResultStatus {
		SUCCESS,
		PARTIAL_SUCCESS
	}
}
