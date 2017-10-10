package com.ctrip.xpipe.redis.console.migration.status;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.migration.*;

import java.lang.reflect.InvocationTargetException;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public enum MigrationStatus {

	Initiated(MigrationInitiatedState.class, ClusterStatus.Lock, false, 0, MigrationStatus.TYPE_INIT),

	Checking(MigrationCheckingState.class, ClusterStatus.Lock, false, 10, MigrationStatus.TYPE_PROCESSING),
	CheckingFail(MigrationCheckingFailState.class, ClusterStatus.Lock, false, 10, MigrationStatus.TYPE_PROCESSING),
	Migrating(MigrationMigratingState.class, ClusterStatus.Migrating, false, 30, MigrationStatus.TYPE_PROCESSING),
	PartialSuccess(MigrationPartialSuccessState.class, ClusterStatus.Migrating, false, 40, MigrationStatus.TYPE_PROCESSING),
	Publish(MigrationPublishState.class, ClusterStatus.TmpMigrated, false, 80, MigrationStatus.TYPE_PROCESSING),
	PublishFail(MigrationPublishState.class, ClusterStatus.TmpMigrated, false, 80, MigrationStatus.TYPE_PROCESSING),
	RollBack(MigrationPartialSuccessRollBackState.class, ClusterStatus.Rollback, false, 30, MigrationStatus.TYPE_PROCESSING),

	Aborted(MigrationAbortedState.class, ClusterStatus.Normal, true, 100, MigrationStatus.TYPE_FAIL),
	Success(MigrationSuccessState.class, ClusterStatus.Normal, true, 100, MigrationStatus.TYPE_SUCCESS),
	ForceEnd(MigrationForceEndState.class, ClusterStatus.Normal, true, 100, MigrationStatus.TYPE_FAIL);

	public static final String TYPE_INIT = "Init";
	public static final String TYPE_PROCESSING = "Processing";
	public static final String TYPE_SUCCESS = "Success";
	public static final String TYPE_FAIL = "Fail";

	private final  Class<MigrationState> classMigrationState;
	private final ClusterStatus clusterStatus;
	private boolean isTerminated;
	private int  	percent;
	private String 	type;

	@SuppressWarnings("unchecked")
	MigrationStatus(Class<?> classMigrationState, ClusterStatus clusterStatus, boolean isTerminated, int percent, String type){
		this.classMigrationState = (Class<MigrationState>) classMigrationState;
		this.clusterStatus = clusterStatus;
		this.isTerminated = isTerminated;
		this.percent = percent;
		this.type = type;
	}
	
	public ClusterStatus getClusterStatus(){
		return clusterStatus;
	}
	
	public MigrationState createMigrationState(MigrationCluster migrationCluster){
		try {
			return classMigrationState.getConstructor(MigrationCluster.class).newInstance(migrationCluster);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new IllegalStateException("[createMigrationState]" + this, e);
		}
	}

	public boolean isTerminated(){
		return isTerminated;
	}

	public int getPercent(){
		return percent;
	}

	public static boolean updateStartTime(MigrationStatus migrationStatus){
		if(migrationStatus == Checking){
			return true;
		}
		return false;
	}

	public String getType() {
		return type;
	}
}
