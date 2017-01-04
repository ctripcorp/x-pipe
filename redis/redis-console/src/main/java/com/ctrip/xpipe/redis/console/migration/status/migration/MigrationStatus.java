package com.ctrip.xpipe.redis.console.migration.status.migration;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public enum MigrationStatus {
	Initiated,
	Checking,
	Migrating,
	Publish,
	Success,
	PartialSuccess,
	ForceEnd,
	RollBack,
	Aborted;
	
	public static boolean isTerminated(MigrationStatus status) {
		return status.equals(Aborted) || status.equals(Success) || status.equals(ForceEnd);
	}
}
