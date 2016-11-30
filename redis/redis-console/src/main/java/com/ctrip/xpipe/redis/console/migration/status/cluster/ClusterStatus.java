package com.ctrip.xpipe.redis.console.migration.status.cluster;

public enum ClusterStatus {
	Normal,
	Lock,
	Migrating,
	TmpMigrated
}
