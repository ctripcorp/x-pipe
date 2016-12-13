package com.ctrip.xpipe.redis.console.migration.status.migration;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationStat {
	MigrationStatus getStat();
	void action();
	void refresh();
	
	MigrationStat nextAfterSuccess();
	MigrationStat nextAfterFail();
	
}
