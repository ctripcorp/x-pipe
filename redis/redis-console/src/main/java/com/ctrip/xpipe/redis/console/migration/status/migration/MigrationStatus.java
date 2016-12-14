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
	Cancelled,
	PartialSuccess,
	ForcePublish,
	TmpEnd,
	ForceFail;
	
	public static boolean isTerminated(String currentStatus) {
		return formatStatus(currentStatus).equals(formatStatus(MigrationStatus.Success)) ||
				formatStatus(currentStatus).equals(formatStatus(MigrationStatus.Cancelled)) ||
				formatStatus(currentStatus).equals(formatStatus(MigrationStatus.ForceFail));
	}
	
	public static boolean isTerminated(MigrationStatus status) {
		return status == MigrationStatus.Cancelled || status == MigrationStatus.Success 
				|| status == MigrationStatus.ForceFail;
	}
	
	private static String formatStatus(String status) {
		return status.trim().toLowerCase();
	}
	
	private static String formatStatus(MigrationStatus status) {
		return status.toString().trim().toLowerCase();
	}
	
	public static boolean isSameStatus(String status, MigrationStatus target) {
		return status.toLowerCase().equals(target.toString().toLowerCase());
	}
}
