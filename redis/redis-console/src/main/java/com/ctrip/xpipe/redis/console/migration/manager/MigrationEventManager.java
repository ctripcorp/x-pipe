package com.ctrip.xpipe.redis.console.migration.manager;

import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationEventManager {
	void addEvent(MigrationEvent event);
	MigrationEvent getEvent(long id);
	void removeEvent(long id);

}
