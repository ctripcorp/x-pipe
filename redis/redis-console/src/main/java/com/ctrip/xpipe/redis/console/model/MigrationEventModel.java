package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;

public class MigrationEventModel implements java.io.Serializable{
	private static final long serialVersionUID = 1L;
	
	private MigrationEventTbl event;
	
	public MigrationEventModel(){}
	
	public MigrationEventTbl getEvent() {
		return event;
	}

	public void setEvent(MigrationEventTbl event) {
		this.event = event;
	}

	@Override
	public String toString() {
		return String.format("%s", event);
	}


	public MigrationRequest createMigrationRequest(String userInfo, String tag){

		MigrationRequest migrationRequest = new MigrationRequest(userInfo);
		migrationRequest.setTag(tag);

		event.getMigrationClusters().forEach((migrationCluster) -> {

			MigrationRequest.ClusterInfo clusterInfo = new MigrationRequest.ClusterInfo(migrationCluster);
			migrationRequest.addClusterInfo(clusterInfo);
		});
		return migrationRequest;

	}
}
