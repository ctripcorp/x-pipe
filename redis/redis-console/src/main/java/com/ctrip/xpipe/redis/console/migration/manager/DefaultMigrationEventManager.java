package com.ctrip.xpipe.redis.console.migration.manager;

import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
@Component
public class DefaultMigrationEventManager implements MigrationEventManager, CrossDcLeaderAware{

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private MigrationEventDao migrationEventDao;

	private ScheduledExecutorService scheduled;

	private Map<Long, MigrationEvent> currentWorkingEvents = new ConcurrentHashMap<>();

	@PostConstruct
	public void defaultMigrationEventManager(){

		scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("EventManagerCleaner"));

		scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
			@Override
			protected void doRun() throws Exception {

				List<Long> finished = new LinkedList<>();

				currentWorkingEvents.forEach((id, migrationEvent) -> {
					if(migrationEvent.isDone()){
						finished.add(id);
					}
				});
				finished.forEach((id) -> removeEvent(id));
			}
		}, 60, 60, TimeUnit.SECONDS);
	}

	@PreDestroy
	public void shutdown() {
		if(scheduled != null) {
			scheduled.shutdownNow();
		}
	}

	@Override
	public void addEvent(MigrationEvent event) {
		event.addObserver(this);
		currentWorkingEvents.put(event.getEvent().getId(), event);
		logger.info("[AddEvent]{}",event.getEvent().getId());
	}

	@Override
	public MigrationEvent getEvent(long eventId) {

		MigrationEvent migrationEvent = currentWorkingEvents.get(eventId);
		if(migrationEvent == null){
			//load it from db
			logger.info("[getEvent][load from db]{}", eventId);
			migrationEvent = loadAndAdd(eventId);
		}

		return migrationEvent;
	}

	private MigrationEvent loadAndAdd(long eventId) {

		try{
			MigrationEvent migrationEvent = migrationEventDao.buildMigrationEvent(eventId);
			addEvent(migrationEvent);
			return migrationEvent;
		}catch(Throwable th){
			logger.error("[load][event]" + eventId, th);
		}
		return null;
	}

	public void removeEvent(long id) {

		logger.info("[removeEvent]{}", id);
		currentWorkingEvents.remove(id);
	}

	private void load() {

		List<Long> unfinishedTasks;
		try{
			unfinishedTasks = migrationEventDao.findAllUnfinished();
		}catch(Exception e){
			logger.warn("[load]{}", e.getMessage());
			return;
		}

		for(Long unfinishedId : unfinishedTasks) {

			try{
				logger.info("[load]{}", unfinishedId);
				loadAndAdd(unfinishedId);
			}catch(Exception e){
				logger.error("[load][fail]" + unfinishedId, e);
			}
		}
	}

	@Override
	public void update(Object args, Observable observable) {

		MigrationEvent event = (MigrationEvent) args;
		if(event.isDone()){
			logger.info("[update][done]{}", event);
		}
	}

	@Override
	public void isCrossDcLeader() {
		logger.info("[isCrossDcLeader][load]");
		load();
	}

	@Override
	public void notCrossDcLeader() {
		logger.info("[notCrossDcLeader][clear]");
		clear();
	}

	private void clear() {
		currentWorkingEvents.clear();
	}
}
