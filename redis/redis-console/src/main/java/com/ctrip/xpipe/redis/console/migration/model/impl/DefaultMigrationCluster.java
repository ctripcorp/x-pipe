package com.ctrip.xpipe.redis.console.migration.model.impl;

import java.util.LinkedList;
import java.util.List;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStat;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;

public class DefaultMigrationCluster extends AbstractObservable implements MigrationCluster, Observer, Observable {
	private MigrationStat currentStat;

	private MigrationClusterTbl migrationCluster;
	private List<MigrationShard> currentlyWorkingMigrationShards = new LinkedList<>();
	private List<MigrationShard> terminatedMigrationShards = new LinkedList<>();

	public DefaultMigrationCluster(MigrationClusterTbl migrationCluster) {
		this.migrationCluster = migrationCluster;
	}

	@Override
	public MigrationStatus getStatus() {
		return currentStat.getStat();
	}

	@Override
	public MigrationClusterTbl getMigrationCluster() {
		return migrationCluster;
	}

	@Override
	public List<MigrationShard> getMigrationShards() {
		List<MigrationShard> allMigrationShards = new LinkedList<>();
		allMigrationShards.addAll(currentlyWorkingMigrationShards);
		allMigrationShards.addAll(terminatedMigrationShards);
		return allMigrationShards;
	}

	@Override
	public List<MigrationShard> getCurrentlyWorkingMigrationShards() {
		return currentlyWorkingMigrationShards;
	}

	@Override
	public List<MigrationShard> getTerminatedMigrationShards() {
		return terminatedMigrationShards;
	}

	@Override
	public void addNewMigrationShard(MigrationShard migrationShard) {
		currentlyWorkingMigrationShards.add(migrationShard);
	}

	@Override
	public void terminateMigrationShard(MigrationShard migrationShard) {
		if (currentlyWorkingMigrationShards.contains(migrationShard)) {
			currentlyWorkingMigrationShards.remove(migrationShard);
			terminatedMigrationShards.add(migrationShard);
		}
	}

	@Override
	public void update(Object args, Observable observable) {
		// TODO update according to MigrationShard Changed

	}

	@Override
	public void process() {
		// TODO
		process(null);
	}

	@Override
	public void updateStat(MigrationStat stat) {
		this.currentStat = stat;
	}

	private void process(MigrationStat nextStat) {
		process(nextStat, false);
	}

	private void process(MigrationStat nextStat, boolean processContinue) {
		updateStat(nextStat);
		if (processContinue) {
			process();
		}
	}

	

}
