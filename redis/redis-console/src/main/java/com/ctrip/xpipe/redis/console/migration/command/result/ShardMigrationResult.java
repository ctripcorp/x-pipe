package com.ctrip.xpipe.redis.console.migration.command.result;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
@SuppressWarnings("serial")
public class ShardMigrationResult  implements Serializable{
	private ShardMigrationResultStatus status;
	private Map<ShardMigrationStep, Pair<Boolean, String>> steps = new ConcurrentHashMap<>(6);
	
	public ShardMigrationResult() {
		status = ShardMigrationResultStatus.FAIL;
	}
	
	public ShardMigrationResultStatus getStatus() {
		return status;
	}
	
	public void setStatus(ShardMigrationResultStatus status) {
		this.status = status;
	}
	
	public Map<ShardMigrationStep, Pair<Boolean, String>> getSteps() {
		return steps;
	}
	
	public boolean stepTerminated(ShardMigrationStep step) {
		return steps.containsKey(step);
	}
	
	public boolean stepSuccess(ShardMigrationStep step) {
		return stepTerminated(step) ? steps.get(step).getLeft() : false;
	}

	public void stepRetry(ShardMigrationStep step) {
		steps.remove(step);
	}

	public void updateStepResult(ShardMigrationStep step, boolean success, String log) {
		steps.put(step, Pair.of(success, log));
	}
	
	public enum ShardMigrationResultStatus {
		SUCCESS,
		FAIL
	}
	
	public enum ShardMigrationStep {
		CHECK,
		MIGRATE_PREVIOUS_PRIMARY_DC,
		MIGRATE_NEW_PRIMARY_DC,
		MIGRATE_OTHER_DC,
		MIGRATE
	}
}
