package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

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
