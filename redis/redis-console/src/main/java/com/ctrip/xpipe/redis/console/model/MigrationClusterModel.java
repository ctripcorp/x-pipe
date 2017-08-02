package com.ctrip.xpipe.redis.console.model;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shyin
 *
 * Dec 13, 2016
 */
public class MigrationClusterModel implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private MigrationClusterInfo migrationCluster;
	
	private List<MigrationShardModel> migrationShards = new LinkedList<>();
	
	public MigrationClusterModel(){
	}
	
	public MigrationClusterInfo getMigrationCluster() {
		return migrationCluster;
	}
	
	public void setMigrationCluster(MigrationClusterInfo migrationCluster) {
		this.migrationCluster = migrationCluster;
	}
	
	public List<MigrationShardModel> getMigrationShards() {
		return migrationShards;
	}
	
	public void addMigrationShard(MigrationShardModel migrationShard) {
		this.migrationShards.add(migrationShard);
	}
	
}
