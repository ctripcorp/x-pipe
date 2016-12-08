package com.ctrip.xpipe.redis.console.migration.status.migration;

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
	
	private static String formatStatus(String status) {
		return status.trim().toLowerCase();
	}
	
	private static String formatStatus(MigrationStatus status) {
		return status.toString().trim().toLowerCase();
	}
}
