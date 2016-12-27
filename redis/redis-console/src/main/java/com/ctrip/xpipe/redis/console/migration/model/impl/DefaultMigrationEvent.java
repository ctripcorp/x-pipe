package com.ctrip.xpipe.redis.console.migration.model.impl;

import java.util.HashMap;
import java.util.Map;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;

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
	public MigrationCluster getMigrationCluster(long clusterId) {
		return migrationClusters.get(clusterId);
	}

	@Override
	public void addMigrationCluster(MigrationCluster migrationClsuter) {
		migrationClsuter.addObserver(this);
		migrationClusters.put(migrationClsuter.getMigrationCluster().getClusterId(), migrationClsuter);
	}

	@Override
	public void update(Object args, Observable observable) {
		if(args instanceof MigrationCluster) {
			if(MigrationStatus.isTerminated(((MigrationCluster) args).getStatus())) {
				// Submit next task according to policy
				processNext();
			}
		}
		int successCnt = 0;
		for(MigrationCluster cluster : migrationClusters.values()) {
			if(cluster.getStatus().equals(MigrationStatus.Success)) {
				++successCnt;
			}
		}
		if(successCnt == migrationClusters.size()) {
			notifyObservers(this);
		}
	}

	private void processNext() {
		for(MigrationCluster migrationCluster : migrationClusters.values()) {
			if(! MigrationStatus.isTerminated(migrationCluster.getStatus())) {
				migrationCluster.process();
			}
		}
	}

}
