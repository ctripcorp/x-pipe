package com.ctrip.xpipe.redis.console.migration.status;

import java.lang.reflect.InvocationTargetException;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationAbortedState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationCheckingState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationForceEndState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationInitiatedState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationMigratingState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPartialSuccessState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPublishState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPartialSuccessRollBackState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationSuccessState;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public enum MigrationStatus {
	
	Initiated(MigrationInitiatedState.class, ClusterStatus.Lock),
	Checking(MigrationCheckingState.class, ClusterStatus.Lock),
	Migrating(MigrationMigratingState.class, ClusterStatus.Migrating),
	PartialSuccess(MigrationPartialSuccessState.class, ClusterStatus.Migrating),
	Publish(MigrationPublishState.class, ClusterStatus.TmpMigrated),
	Aborted(MigrationAbortedState.class, ClusterStatus.Normal),
	Success(MigrationSuccessState.class, ClusterStatus.Normal),
	ForceEnd(MigrationForceEndState.class, ClusterStatus.Normal),
	RollBack(MigrationPartialSuccessRollBackState.class, ClusterStatus.Rollback);
	
	private final  Class<MigrationState> classMigrationState;
	private final ClusterStatus clusterStatus;
	
	@SuppressWarnings("unchecked")
	MigrationStatus(Class<?> classMigrationState, ClusterStatus clusterStatus){
		this.classMigrationState = (Class<MigrationState>) classMigrationState;
		this.clusterStatus = clusterStatus;
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

	public static boolean isTerminated(MigrationStatus status) {
		return status.equals(Aborted) || status.equals(Success) || status.equals(ForceEnd);
	}

}
