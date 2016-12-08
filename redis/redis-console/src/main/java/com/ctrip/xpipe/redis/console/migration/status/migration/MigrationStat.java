package com.ctrip.xpipe.redis.console.migration.status.migration;

public interface MigrationStat {
	MigrationStatus getStat();
	void action();
	void refresh();
	
	MigrationStat nextAfterSuccess();
	MigrationStat nextAfterFail();
	
}
