package com.ctrip.xpipe.redis.console.migration.status.cluster;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public enum ClusterStatus {
	Normal,
	Lock,
	Migrating,
	TmpMigrated;
	
	public static boolean isSameClusterStatus(String source, ClusterStatus target) {
		return source.toLowerCase().equals(target.toString().toLowerCase());
	}
}
