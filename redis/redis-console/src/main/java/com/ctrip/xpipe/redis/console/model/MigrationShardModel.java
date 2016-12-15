package com.ctrip.xpipe.redis.console.model;

import java.io.Serializable;

/**
 * @author shyin
 *
 * Dec 13, 2016
 */
public class MigrationShardModel implements Serializable{
	private static final long serialVersionUID = 1L;

	private MigrationShardTbl migrationShard;
	
	public MigrationShardModel(){}

	public MigrationShardTbl getMigrationShard() {
		return migrationShard;
	}

	public void setMigrationShard(MigrationShardTbl migrationShard) {
		this.migrationShard = migrationShard;
	}

}
