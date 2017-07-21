package com.ctrip.xpipe.redis.console.service.migration;

import java.util.Date;
import java.util.List;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterModel;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterActiveDcNotRequest;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterMigratingNow;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import com.ctrip.xpipe.redis.console.service.migration.impl.TryMigrateResult;

public interface MigrationService {

    MigrationEventTbl find(long id);

    List<MigrationEventTbl> findAll();

    MigrationClusterTbl findMigrationCluster(long eventId, long clusterId);

    void updateMigrationClusterStartTime(long migrationClusterId, Date startTime);

    void updateStatusAndEndTimeById(long migrationClusterId, MigrationStatus status, Date endTime);

    void updatePublishInfoById(long migrationClusterId, String publishInfo);

    MigrationClusterTbl findLatestUnfinishedMigrationCluster(long clusterId);

    List<MigrationShardTbl> findMigrationShards(long migrationClusterId);

    List<MigrationClusterModel> getMigrationClusterModel(long eventId);

    void updateMigrationShardLogById(long id, String log);

    TryMigrateResult tryMigrate(String clusterName, String fromIdc) throws ClusterNotFoundException, ClusterActiveDcNotRequest, ClusterMigratingNow;

    Long createMigrationEvent(MigrationRequest request);

    void continueMigrationCluster(long eventId, long clusterId);

    void continueMigrationEvent(long eventId);

    MigrationEvent getMigrationEvent(long eventId);

    void cancelMigrationCluster(long eventId, long clusterId);

    MigrationCluster rollbackMigrationCluster(long eventId, long clusterId) throws ClusterNotFoundException;

    MigrationCluster rollbackMigrationCluster(long eventId, String clusterName) throws ClusterNotFoundException;

    void forcePublishMigrationCluster(long eventId, long clusterId);

    void forceEndMigrationClsuter(long eventId, long clusterId);
}
