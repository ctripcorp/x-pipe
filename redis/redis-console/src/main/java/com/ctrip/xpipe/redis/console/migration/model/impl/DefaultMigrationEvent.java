package com.ctrip.xpipe.redis.console.migration.model.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.google.common.collect.Lists;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class DefaultMigrationEvent extends AbstractObservable implements MigrationEvent, Observer {
	private MigrationEventTbl event;
	private Map<Long, MigrationCluster> migrationClusters = new HashMap<>();

	public DefaultMigrationEvent(MigrationEventTbl event) {
		this.event = event;
	}
	
	@Override
	public MigrationEventTbl getEvent() {
		return event;
	}

	@Override
	public long getMigrationEventId() {
		return event.getId();
	}

	@Override
	public MigrationCluster getMigrationCluster(long clusterId) {
		return migrationClusters.get(clusterId);
	}
	
	@Override
	public List<MigrationCluster> getMigrationClusters() {
		return Lists.newLinkedList(migrationClusters.values());
	}

	@Override
	public void addMigrationCluster(MigrationCluster migrationClsuter) {
		migrationClsuter.addObserver(this);
		migrationClusters.put(migrationClsuter.getMigrationCluster().getClusterId(), migrationClsuter);
	}

	@Override
	public void update(Object args, Observable observable) {
		if(args instanceof MigrationCluster) {
			if(((MigrationCluster) args).getStatus().isTerminated()) {
				// Submit next task according to policy
				processNext();
			}
		}
		int finishedCnt = 0;
		for(MigrationCluster cluster : migrationClusters.values()) {
			if(cluster.getStatus().isTerminated()) {
				++finishedCnt;
			}
		}
		if(finishedCnt == migrationClusters.size()) {
			notifyObservers(this);
		}
	}

	private void processNext() {
		for(MigrationCluster migrationCluster : migrationClusters.values()) {
			if(!migrationCluster.getStatus().isTerminated()) {
				migrationCluster.process();
			}
		}
	}

}
