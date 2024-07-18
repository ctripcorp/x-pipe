package com.ctrip.xpipe.redis.console.service.migration;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.MigrationProgress;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import com.ctrip.xpipe.redis.console.service.migration.impl.TryMigrateResult;

import java.util.Date;
import java.util.List;
import java.util.Set;

public interface MigrationService {

    long countAll();

    long countAllByCluster(long clusterId);

    long countAllByOperator(String operator);

    long countAllByStatus(String status);

    long countAllWithoutTestCluster();

    List<MigrationModel> find(long size, long offset);

    List<MigrationModel> findByCluster(long clusterId, long size, long offset);

    List<MigrationModel> findByOperator(String operator, long size, long offset);

    List<MigrationModel> findByStatus(String status, long size, long offset);

    List<MigrationModel> findWithoutTestClusters(long size, long offset);

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

    TryMigrateResult tryMigrate(String clusterName, String fromIdc, String toIdc) throws ClusterNotFoundException, MigrationNotSupportException, ClusterActiveDcNotRequest, ClusterMigratingNow, ToIdcNotFoundException, MigrationSystemNotHealthyException, ClusterMigratingNowButMisMatch;

    Long createMigrationEvent(MigrationRequest request);

    void continueMigrationCluster(long eventId, long clusterId);

    void continueMigrationEvent(long eventId) throws Exception;

    MigrationEvent getMigrationEvent(long eventId);

    void cancelMigrationCluster(long eventId, long clusterId);

    MigrationCluster rollbackMigrationCluster(long eventId, long clusterId) throws ClusterNotFoundException;

    MigrationCluster rollbackMigrationCluster(long eventId, String clusterName) throws ClusterNotFoundException;

    void forceProcessMigrationCluster(long eventId, long clusterId);

    void forceEndMigrationCluster(long eventId, long clusterId);

    MigrationSystemAvailableChecker.MigrationSystemAvailability getMigrationSystemAvailability();

    RetMessage getMigrationSystemHealth();

    MigrationProgress buildMigrationProgress(int hours);

    void updateMigrationStatus(MigrationCluster migrationCluster, MigrationStatus status);

    Set<String> getLatestMigrationOperators(int hours);

    List<MigrationClusterTbl> getLatestMigrationClusters(int seconds);

    List<MigrationClusterTbl> fetchMigrationClusters(Set<String> clusters, long from, long to);
}
