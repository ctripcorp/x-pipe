package com.ctrip.xpipe.redis.console.migration.manager;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
@Component
public class DefaultMigrationEventManager implements MigrationEventManager {
	
	private Map<Long, MigrationEvent> currentWorkingEvents = new HashMap<>();
	
	@PostConstruct
	private void postConstruct() {
		load();
	}

	@Override
	public void addEvent(MigrationEvent event) {
		currentWorkingEvents.put(event.getEvent().getId(), event);
	}

	@Override
	public MigrationEvent getEvent(long id) {
		return currentWorkingEvents.get(id);
	}

	@Override
	public void removeEvent(long id) {
		currentWorkingEvents.remove(id);
		
	}
	
	private void load() {
		// TODO : initialize currently working migration events
		
	}

	
}
