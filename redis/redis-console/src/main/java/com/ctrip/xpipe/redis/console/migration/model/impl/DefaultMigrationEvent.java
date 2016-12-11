package com.ctrip.xpipe.redis.console.migration.model.impl;

import java.util.HashMap;
import java.util.Map;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;

public class DefaultMigrationEvent implements MigrationEvent, Observer {
	
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
	}

	private void processNext() {
		for(MigrationCluster migrationCluster : migrationClusters.values()) {
			if(! MigrationStatus.isTerminated(migrationCluster.getStatus())) {
				migrationCluster.process();
			}
		}
	}

}
