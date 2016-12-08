package com.ctrip.xpipe.redis.console.migration.status.migration;

public interface MigrationStat {
	MigrationStatus getStat();
	boolean action();
}
