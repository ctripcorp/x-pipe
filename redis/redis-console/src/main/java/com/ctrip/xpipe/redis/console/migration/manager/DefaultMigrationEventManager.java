package com.ctrip.xpipe.redis.console.migration.manager;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
@Component
public class DefaultMigrationEventManager implements MigrationEventManager {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private MigrationEventDao migrationEventDao;
	private boolean initiated = false;
	
	private Map<Long, MigrationEvent> currentWorkingEvents = new HashMap<>();


	@Override
	public void addEvent(MigrationEvent event) {
		assureInit();
		event.addObserver(this);
		currentWorkingEvents.put(event.getEvent().getId(), event);
		logger.info("[AddEvent]{}",event.getEvent().getId());
	}

	@Override
	public MigrationEvent getEvent(long id) {
		assureInit();
		return currentWorkingEvents.get(id);
	}

	@Override
	public void removeEvent(long id) {
		assureInit();
		currentWorkingEvents.remove(id);
		logger.info("[RemoveEvent]{}", id);
	}

	private void assureInit() {
		if(!initiated) {
			synchronized (this) {
				if(!initiated) {
					this.initiated = true;
					load();
				}
			}
		}
	}
	
	private void load() {
		List<MigrationEventTbl> unfinishedTasks = migrationEventDao.findAllUnfinished();
		Set<Long> unfinishedIds = new HashSet<>();
		for(MigrationEventTbl unfinished : unfinishedTasks) {
			unfinishedIds.add(unfinished.getId());
		}
		
		for(Long id : unfinishedIds) {
			addEvent(migrationEventDao.buildMigrationEvent(id));
		}
	}

	@Override
	public void update(Object args, Observable observable) {
		removeEvent(((MigrationEvent) args).getEvent().getId()); 
	}
}
