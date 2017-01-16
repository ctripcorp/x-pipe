package com.ctrip.xpipe.redis.console.migration.status.migration;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationState {
	MigrationStatus getStatus();
	void action();
	void refresh();
	
	MigrationState nextAfterSuccess();
	MigrationState nextAfterFail();
	
}
