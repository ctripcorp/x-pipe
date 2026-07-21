package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.MigrationProgress;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.entity.ClusterEntity;
import com.ctrip.xpipe.redis.console.entity.MigrationBiClusterEntity;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.job.retry.RetryCondition;
import com.ctrip.xpipe.redis.console.job.retry.RetryNTimesOnCondition;
import com.ctrip.xpipe.redis.console.migration.MigrationResources;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.checker.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.repository.ClusterRepository;
import com.ctrip.xpipe.redis.console.repository.MigrationBiClusterRepository;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.ctrip.xpipe.redis.console.service.migration.support.HeteroMigrationSupport;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.rmi.ServerException;
import java.util.*;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;


@Service
public class MigrationServiceImpl extends AbstractConsoleService<MigrationEventTblDao> implements MigrationService {

    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    @Lazy
    private MigrationEventDao migrationEventDao;

    @Autowired
    @Lazy
    private MigrationEventManager migrationEventManager;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private DcService dcService;

    @Autowired
    private DcCache dcCache;

    @Autowired
    private MigrationClusterDao migrationClusterDao;

    @Autowired
    private MigrationSystemAvailableChecker checker;

    @Autowired
    private ConfigService configService;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private HeteroMigrationSupport heteroMigrationSupport;

    @Autowired
    private ConsoleConfig config;

    @Autowired
    private DcRelationsService dcRelationsService;

    @Autowired(required = false)
    private BeaconManager beaconManager;

    @Resource
    private ClusterRepository clusterRepository;

    @Resource
    private MigrationBiClusterRepository migrationBiClusterRepository;

    private MigrationShardTblDao migrationShardTblDao;

    @Resource(name = MigrationResources.MIGRATION_POST_BEACON_EXECUTOR)
    private Executor postMigrateBeaconExecutor;

    @PostConstruct
    private void postConstruct() throws ServerException {
        try {
            migrationShardTblDao = ContainerLoader.getDefaultContainer().lookup(MigrationShardTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct dao.");
        }
    }

    @Override
    public MigrationEventTbl find(final long id) {
        return queryHandler.handleQuery(new DalQuery<MigrationEventTbl>() {
            @Override
            public MigrationEventTbl doQuery() throws DalException {
                return dao.findByPK(id, MigrationEventTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public long countAll() {
        return queryHandler.handleQuery(new DalQuery<Long>() {
            @Override
            public Long doQuery() throws DalException {
                return dao.countAll(MigrationEventTblEntity.READSET_COUNT).getCount();
            }
        });
    }

    @Override
    public long countAllByCluster(long clusterId) {
        return migrationClusterDao.countAllByCluster(clusterId);
    }

    @Override
    public long countAllByOperator(String operator) {
        return migrationClusterDao.countAllByOperator(operator);
    }

    @Override
    public long countAllByStatus(String status) {
        return migrationClusterDao.countAllByStatus(status);
    }

    @Override
    public long countAllWithoutTestCluster() {
        return migrationClusterDao.countAllEventsWithoutTestClusters();
    }

    @Override
    public List<MigrationModel> find(long size, long offset) {
        List<MigrationClusterTbl> migrationClusterList = migrationClusterDao.find(size, offset);
        return aggregateClusterByMigration(migrationClusterList);
    }

    @Override
    public List<MigrationModel> findByCluster(long clusterId, long size, long offset) {
        List<MigrationClusterTbl> migrationClusterList =
                migrationClusterDao.findByCluster(clusterId, size, offset);
        return aggregateClusterByMigration(migrationClusterList);
    }

    @Override
    public List<MigrationModel> findByOperator(String operator, long size, long offset) {
        List<MigrationClusterTbl> migrationClusterList =
                migrationClusterDao.findByOperator(operator, size, offset);
        return aggregateClusterByMigration(migrationClusterList);
    }

    @Override
    public List<MigrationModel> findByStatus(String status, long size, long offset) {
        List<MigrationClusterTbl> migrationClusterList =
                migrationClusterDao.findByStatus(status, size, offset);
        return aggregateClusterByMigration(migrationClusterList);
    }

    @Override
    public List<MigrationModel> findWithoutTestClusters(long size, long offset) {
        List<Long> migrationEventIds =
                migrationClusterDao.findAllEventsWithoutTestCluster(size, offset)
                        .stream().map(MigrationClusterTbl::getMigrationEventId).collect(Collectors.toList());
        if (migrationEventIds.isEmpty()) return Collections.emptyList();
        List<MigrationClusterTbl> migrationClusterList = migrationClusterDao.findByMigEventIds(migrationEventIds);
        return aggregateClusterByMigration(migrationClusterList);
    }

    private List<MigrationModel> aggregateClusterByMigration(List<MigrationClusterTbl> migrationClusterTblList) {
        Map<Long, List<MigrationClusterTbl> > clusterMap = new LinkedHashMap<>();

        for (MigrationClusterTbl migrationCluster: migrationClusterTblList) {
            MigrationEventTbl event = migrationCluster.getMigrationEvent();

            if (!clusterMap.containsKey(event.getId())) {
                clusterMap.put(event.getId(), new LinkedList<>());
            }

            clusterMap.get(event.getId()).add(migrationCluster);
        }

        Iterator<Map.Entry<Long, List<MigrationClusterTbl> > > iterator = clusterMap.entrySet().iterator();
        List<MigrationModel> modals = new LinkedList<>();

        while (iterator.hasNext()) {
            List<MigrationClusterTbl> clusters = iterator.next().getValue();
            modals.add(MigrationModel.createFromMigrationClusters(clusters));
        }

        return modals;
    }

    @Override
    public List<MigrationEventTbl> findAll() {
        return queryHandler.handleQuery(new DalQuery<List<MigrationEventTbl>>() {
            @Override
            public List<MigrationEventTbl> doQuery() throws DalException {
                return dao.findAll(MigrationEventTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public MigrationClusterTbl findMigrationCluster(final long eventId, final long clusterId) {

        return migrationClusterDao.findByEventIdAndClusterId(eventId, clusterId);
    }

    @Override
    public void updateMigrationClusterStartTime(long migrationClusterId, Date startTime) {
        migrationClusterDao.updateStartTime(migrationClusterId, startTime);
    }

    @Override
    public void updateStatusAndEndTimeById(long migrationClusterId, MigrationStatus status, Date endTime) {
        migrationClusterDao.updateStatusAndEndTimeById(migrationClusterId, status, endTime);
    }

    @Override
    public void updatePublishInfoById(long migrationClusterId, String publishInfo) {
        migrationClusterDao.updatePublishInfoById(migrationClusterId, publishInfo);
    }

    @Override
    public MigrationClusterTbl findLatestUnfinishedMigrationCluster(final long clusterId) {

        List<MigrationClusterTbl> unfinishedByClusterId = migrationClusterDao.findUnfinishedByClusterId(clusterId);

        if (unfinishedByClusterId.size() == 0) {
            return null;
        }

        if (unfinishedByClusterId.size() > 1) {
            alertManager.alert(String.valueOf(clusterId), null, null, ALERT_TYPE.MIGRATION_MANY_UNFINISHED, String.format("[count]%d", unfinishedByClusterId.size()));
        }
        return unfinishedByClusterId.get(unfinishedByClusterId.size() - 1);
    }

    @Override
    public List<MigrationShardTbl> findMigrationShards(final long migrationClusterId) {
        return queryHandler.handleQuery(new DalQuery<List<MigrationShardTbl>>() {
            @Override
            public List<MigrationShardTbl> doQuery() throws DalException {
                return migrationShardTblDao.findByMigrationClusterId(migrationClusterId, MigrationShardTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<MigrationClusterModel> getMigrationClusterModel(long eventId) {
        return migrationEventDao.getMigrationCluster(eventId);
    }

    @Override
    public Long createMigrationEvent(MigrationRequest request) {
        preCheck(request);
        MigrationEvent event = migrationEventDao.createMigrationEvent(request);
        migrationEventManager.addEvent(event);
        return event.getEvent().getId();
    }

    private void preCheck(MigrationRequest request) {
        List<MigrationRequest.ClusterInfo> clusterinfos = request.getRequestClusters();
        for(MigrationRequest.ClusterInfo clusterInfo : clusterinfos) {
            if(clusterInfo.getToDcId() < 0 || !isDestDcIdValid(clusterInfo.getClusterId(), clusterInfo.getToDcId())) {
                throw new BadRequestException("Target DC Id Illegal for cluster: " + clusterInfo.getClusterId());
            }

            ClusterTbl clusterTbl;
            ClusterType clusterType;
            if (StringUtil.isEmpty(clusterInfo.getClusterName())) {
                clusterTbl = clusterService.find(clusterInfo.getClusterId());
                clusterType = ClusterType.lookup(clusterTbl.getClusterType());
            } else {
                clusterTbl = clusterService.find(clusterInfo.getClusterName());
                clusterType = metaCache.getClusterType(clusterInfo.getClusterName());
            }

            if (null == clusterType || !clusterType.supportMigration())
                throw new BadRequestException(String.format("cluster %s type %s not support migration", clusterInfo.getClusterName(), clusterType));

            if (heteroMigrationSupport.isHeteroCluster(clusterTbl)) {
                preCheckHeteroCluster(clusterInfo, clusterTbl);
            }
        }
    }

    private void preCheckHeteroCluster(MigrationRequest.ClusterInfo clusterInfo, ClusterTbl clusterTbl) {
        String clusterName = clusterTbl.getClusterName();
        DcTbl sourceDc = dcCache.find(clusterInfo.getFromDcId());
        if (sourceDc == null) {
            logger.warn("[preCheckHeteroCluster][{}] illegal source DC id: {}", clusterName, clusterInfo.getFromDcId());
            throw new BadRequestException(String.format("HETERO cluster %s illegal source DC id: %d",
                    clusterName, clusterInfo.getFromDcId()));
        }
        AzGroupClusterEntity azGroupCluster = heteroMigrationSupport.resolveMigrationAzGroupClusters(
                Collections.singletonList(clusterTbl.getId()), sourceDc.getDcName()).get(clusterTbl.getId());
        if (azGroupCluster == null) {
            logger.warn("[preCheckHeteroCluster][{}] cannot resolve ONE_WAY az group for sourceDc {}",
                    clusterName, sourceDc.getDcName());
            throw new BadRequestException(String.format("HETERO cluster %s cannot resolve ONE_WAY az group for sourceDc %s",
                    clusterName, sourceDc.getDcName()));
        }
        Long activeAzId = azGroupCluster.getActiveAzId();
        if (activeAzId == null) {
            logger.warn("[preCheckHeteroCluster][{}] az group active_az_id is null", clusterName);
            throw new BadRequestException(String.format("HETERO cluster %s az group active_az_id is not configured",
                    clusterName));
        }
        if (clusterInfo.getFromDcId() != activeAzId) {
            logger.warn("[preCheckHeteroCluster][{}] sourceDc must be az group active_dc_id, expected {} but got {}",
                    clusterName, activeAzId, clusterInfo.getFromDcId());
            throw new BadRequestException(String.format(
                    "HETERO cluster %s sourceDc must be az group active_dc_id, expected %d but got %d",
                    clusterName, activeAzId, clusterInfo.getFromDcId()));
        }
        if (clusterInfo.getAzGroupClusterId() != null
                && !clusterInfo.getAzGroupClusterId().equals(azGroupCluster.getId())) {
            logger.warn("[preCheckHeteroCluster][{}] azGroupClusterId mismatch, expected {} but got {}",
                    clusterName, azGroupCluster.getId(), clusterInfo.getAzGroupClusterId());
            throw new BadRequestException(String.format(
                    "HETERO cluster %s azGroupClusterId mismatch, expected %d but got %d",
                    clusterName, azGroupCluster.getId(), clusterInfo.getAzGroupClusterId()));
        }
        DcTbl targetDc = dcCache.find(clusterInfo.getToDcId());
        if (targetDc == null) {
            logger.warn("[preCheckHeteroCluster][{}] illegal target DC id: {}", clusterName, clusterInfo.getToDcId());
            throw new BadRequestException(String.format("HETERO cluster %s illegal target DC id: %d",
                    clusterName, clusterInfo.getToDcId()));
        }
        if (!heteroMigrationSupport.isSameAzGroup(
                clusterTbl.getId(), sourceDc.getDcName(), targetDc.getDcName())) {
            logger.warn("[preCheckHeteroCluster][{}] target DC {} must be in same az group as source DC {}",
                    clusterName, targetDc.getDcName(), sourceDc.getDcName());
            throw new BadRequestException(String.format(
                    "HETERO cluster %s target DC %s must be in same az group as source DC %s",
                    clusterName, targetDc.getDcName(), sourceDc.getDcName()));
        }
    }

    private boolean isDestDcIdValid(long clusterId, long dcId) {
        try {
            DcClusterTbl dcClusterTbl = dcClusterService.find(dcId, clusterId);
            if(dcClusterTbl == null) {
                return false;
            }
        } catch (Exception e) {
            logger.error("[isDestDcIdValid]", e);
            return false;
        }
        return true;
    }

    @Override
    public void updateMigrationShardLogById(final long id, final String log) {

        MigrationShardTbl migrationShardTbl = new MigrationShardTbl();
        migrationShardTbl.setId(id);
        migrationShardTbl.setLog(log);

        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return migrationShardTblDao.updateLogById(migrationShardTbl, MigrationShardTblEntity.UPDATESET_FULL);
            }
        });
    }

    @Override
    public void continueMigrationCluster(final long eventId, final long clusterId) {

        if (isMigrationClusterExist(eventId, clusterId)) {
            migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).process();
        } else {
            throw new IllegalArgumentException(String.format("event %d, cluster:%d not found", eventId, clusterId));
        }
    }

    @Override
    public void continueMigrationEvent(long id) {

        logger.info("[continueMigrationEvent]{}", id);
        MigrationEvent event = getEvent(id);
        if (event == null) {
            throw new IllegalArgumentException("event not found:" + id);
        }
        event.process();
    }

    @Override
    public MigrationEvent getMigrationEvent(long eventId) {
        return getEvent(eventId);
    }


    @Override
    public MigrationCluster rollbackMigrationCluster(long eventId, long clusterId) throws ClusterNotFoundException {

        MigrationEvent event = getEvent(eventId);
        if (event == null) {
            throw new IllegalArgumentException("event not found:" + eventId);
        }
        return event.rollbackCluster(clusterId);
    }

    @Override
    public MigrationCluster rollbackMigrationCluster(long eventId, String clusterName) throws ClusterNotFoundException {

        MigrationEvent event = getEvent(eventId);
        if (event == null) {
            throw new IllegalArgumentException("event not found:" + eventId);
        }

        return event.rollbackCluster(clusterName);
    }


    @Override
    public void cancelMigrationCluster(long eventId, long clusterId) {
        if (isMigrationClusterExist(eventId, clusterId)) {
            migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).cancel();
        }
    }


    private MigrationEvent getEvent(long eventId) {
        return migrationEventManager.getEvent(eventId);
    }

    @Override
    public void forceProcessMigrationCluster(long eventId, long clusterId) {
        if (isMigrationClusterExist(eventId, clusterId)) {
            migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).forceProcess();
        }
    }

    @Override
    public void forceEndMigrationCluster(long eventId, long clusterId) {
        if (isMigrationClusterExist(eventId, clusterId)) {
            migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).forceEnd();
        }
    }

    @Override
    public MigrationSystemAvailableChecker.MigrationSystemAvailability getMigrationSystemAvailability() {
        MigrationSystemAvailableChecker.MigrationSystemAvailability availability =  checker.getResult();
        if(availability.isAvaiable() && !availability.isWarning()) {
            if (System.currentTimeMillis() - availability.getTimestamp() >= TimeUnit.MINUTES.toMillis(2)) {
                String message = "not updated over 2 min\n";
                availability.addWarningMessage(message);
                alertManager.alert("", "", new HostPort(), ALERT_TYPE.MIGRATION_SYSTEM_CHECK_OVER_DUE, message);
            }
        }
        return availability;
    }

    private boolean isMigrationClusterExist(long eventId, long clusterId) {
        boolean ret = false;
        if (null != migrationEventManager.getEvent(eventId)) {
            if (null != migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId)) {
                ret = true;
            }
        }
        return ret;
    }

    @Override
    public TryMigrateResult tryMigrate(String clusterName, String fromIdc, String toIdc)
            throws ClusterNotFoundException, MigrationNotSupportException, ClusterActiveDcNotRequest, ClusterMigratingNow, ToIdcNotFoundException, MigrationSystemNotHealthyException, ClusterMigratingNowButMisMatch {

        if(!checker.getResult().isAvaiable() && !configService.ignoreMigrationSystemAvailability()) {
            throw new MigrationSystemNotHealthyException(checker.getResult().getMessage());
        }
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            throw new ClusterNotFoundException(clusterName);
        }
        if (!ClusterType.lookup(clusterTbl.getClusterType()).supportMigration()) {
            throw new MigrationNotSupportException(clusterName);
        }

        MigrationClusterTbl unfinished = findLatestUnfinishedMigrationCluster(clusterTbl.getId());
        if (unfinished != null) {
            long migrationEventId = unfinished.getMigrationEventId();
            long fromDcId = unfinished.getSourceDcId();
            long toDcId = unfinished.getDestinationDcId();
            String realFromIdc = dcService.getDcName(fromDcId);
            String realToIdc = dcService.getDcName(toDcId);
            if(fromIdc == null || fromIdc.equalsIgnoreCase(realFromIdc)){
                if(toIdc == null || toIdc.equalsIgnoreCase(realToIdc)){
                    logger.info("[tryMigrate][already migrating]{},real({}->{}), request({}->{})",
                            migrationEventId, realFromIdc, realToIdc, fromIdc, toIdc);
                    throw new ClusterMigratingNow(clusterName, realFromIdc, realToIdc, unfinished.getMigrationEventId());
                }
            }
            throw new ClusterMigratingNowButMisMatch(clusterName, realFromIdc, realToIdc, unfinished.getMigrationEventId(), fromIdc, toIdc);
        }

        AzGroupClusterEntity azGroupCluster = null;
        DcTbl activeDc;
        if (heteroMigrationSupport.isHeteroCluster(clusterTbl)) {
            azGroupCluster = resolveHeteroAzGroupClusterForTryMigrate(clusterTbl, fromIdc);
            activeDc = dcCache.find(azGroupCluster.getActiveAzId());
            if (activeDc == null) {
                throw new ClusterActiveDcNotRequest(clusterName, fromIdc, "active az dc not found");
            }
            if (!fromIdc.equalsIgnoreCase(activeDc.getDcName())) {
                throw new ClusterActiveDcNotRequest(clusterName, fromIdc, activeDc.getDcName());
            }
        } else {
            activeDc = dcService.find(clusterTbl.getActivedcId());
            if (fromIdc != null && !fromIdc.equalsIgnoreCase(activeDc.getDcName())) {
                throw new ClusterActiveDcNotRequest(clusterName, fromIdc, activeDc.getDcName());
            }
        }

        List<DcTbl> clusterRelatedDc = dcService.findClusterRelatedDc(clusterName);
        logger.debug("[tryMigrate][clusterRelatedDc]{}", clusterRelatedDc);

        DcTbl toDc = findToDc(clusterTbl, fromIdc, toIdc, clusterRelatedDc, azGroupCluster);
        return new TryMigrateResult(clusterTbl, activeDc, toDc);
    }

    private AzGroupClusterEntity resolveHeteroAzGroupClusterForTryMigrate(ClusterTbl clusterTbl, String fromIdc)
            throws ClusterActiveDcNotRequest {
        String clusterName = clusterTbl.getClusterName();
        if (StringUtil.isEmpty(fromIdc)) {
            throw new ClusterActiveDcNotRequest(clusterName, fromIdc, "fromIdc required for HETERO cluster");
        }
        AzGroupClusterEntity azGroupCluster = heteroMigrationSupport.resolveMigrationAzGroupClusters(
                Collections.singletonList(clusterTbl.getId()), fromIdc).get(clusterTbl.getId());
        if (azGroupCluster == null) {
            logger.warn("[tryMigrate][{}] cannot resolve ONE_WAY az group for sourceDc {}", clusterName, fromIdc);
            throw new ClusterActiveDcNotRequest(clusterName, fromIdc, "cannot resolve ONE_WAY az group");
        }
        if (azGroupCluster.getActiveAzId() == null) {
            logger.warn("[tryMigrate][{}] az group active_az_id is null", clusterName);
            throw new ClusterActiveDcNotRequest(clusterName, fromIdc, "az group active_az_id is not configured");
        }
        return azGroupCluster;
    }

    @Override
    public RetMessage getMigrationSystemHealth() {
        MigrationSystemAvailableChecker.MigrationSystemAvailability availability = this.getMigrationSystemAvailability();
        if(availability.isAvaiable()) {
            if(!availability.isWarning()) {
                logger.debug("[getMigrationSystemHealthStatus][good]");
                return RetMessage.createSuccessMessage();
            } else {
                logger.debug("[getMigrationSystemHealthStatus][warned]");
                return RetMessage.createWarningMessage(availability.getMessage());
            }
        }
        if(configService.ignoreMigrationSystemAvailability()) {
            logger.warn("[getMigrationSystemHealthStatus][warn]{}", availability.getMessage());
            return RetMessage.createWarningMessage(availability.getMessage());
        } else {
            logger.error("[getMigrationSystemHealthStatus][warn]{}", availability.getMessage());
            return RetMessage.createFailMessage(availability.getMessage());
        }
    }

    @Override
    public MigrationProgress buildMigrationProgress(int hours) {
        MigrationProgress progress = new MigrationProgress();
        long totalMigrationSeconds = 0;

        List<MigrationClusterTbl> migrationClusterTbls = migrationClusterDao
                .findLatestMigrationClusters(DateTimeUtils.getHoursBeforeDate(new Date(), hours));

        for (MigrationClusterTbl migrationClusterTbl : migrationClusterTbls) {
            progress.addMigrationCluster(migrationClusterTbl);
            MigrationStatus status = MigrationStatus.valueOf(migrationClusterTbl.getStatus());
            if (MigrationStatus.Success.equals(status)) {
                if (null == migrationClusterTbl.getEndTime() || null == migrationClusterTbl.getStartTime()) {
                    logger.info("[buildMigrationProgress][{}] no time but success", migrationClusterTbl.getId());
                }
                long durationMilli = migrationClusterTbl.getEndTime().getTime() - migrationClusterTbl.getStartTime().getTime();
                totalMigrationSeconds += TimeUnit.MILLISECONDS.toSeconds(durationMilli);
            }
        }
        if (progress.getSuccess() > 0) progress.setAvgMigrationSeconds(totalMigrationSeconds / progress.getSuccess());
        progress.setActiveDcs(metaCache.getAllDcMigratableClustersCnt());

        return progress;
    }

    @Override
    @DalTransaction
    public void updateMigrationStatus(MigrationCluster migrationCluster, MigrationStatus status) {
        try {
            MigrationClusterTbl migrationClusterTbl = migrationCluster.getMigrationCluster();
            tryUpdateStartTime(migrationCluster.clusterName(), migrationClusterTbl.getId(), status);
            updateStorageClusterStatus(migrationClusterTbl.getMigrationEventId(),
                    migrationCluster.getCurrentCluster().getId(), migrationCluster.clusterName(), status.getClusterStatus());
            updateStatusAndEndTimeById(migrationClusterTbl.getId(), status, new Date());
        } catch (Exception e) {
            logger.error("[updateMigrationStatus] ", e);
            throw new com.ctrip.xpipe.redis.console.exception.ServerException(e.getMessage());
        }
    }

    private void tryUpdateStartTime(String clusterName, long migrationClusterId, MigrationStatus migrationStatus) {
        try{
            if(MigrationStatus.updateStartTime(migrationStatus)){
                logger.info("[tryUpdateStartTime][doUpdate]{}, {}, {}", migrationClusterId, clusterName, migrationStatus);
                updateMigrationClusterStartTime(migrationClusterId, new Date());
            }
        }catch (Exception e){
            logger.error("[tryUpdateStartTime]" + clusterName, e);
        }
    }

    @VisibleForTesting
    protected void updateStorageClusterStatus(long migrationEventId, long clusterId, String clusterName, ClusterStatus clusterStatus) throws Exception {
        logger.info("[updateStorageClusterStatus][updatedb]{}, {}", clusterName, clusterStatus);
        RetryTemplate<String> retryTemplate = new RetryNTimesOnCondition<>(new RetryCondition.AbstractRetryCondition<String>() {
            @Override
            public boolean isSatisfied(String s) {
                return ClusterStatus.isSameClusterStatus(s, clusterStatus);
            }

            @Override
            public boolean isExceptionExpected(Throwable th) {
                if(th instanceof TimeoutException)
                    return true;
                return false;
            }
        }, 3);
        retryTemplate.execute(new AbstractCommand<String>() {
            @Override
            protected void doExecute() throws Exception {
                try {
                    clusterService.updateStatusById(clusterId, clusterStatus, migrationEventId);
                    ClusterTbl newCluster = clusterService.find(clusterName);
                    future().setSuccess(newCluster.getStatus());
                } catch (Exception e) {
                    future().setFailure(e.getCause());
                }
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "update cluster status";
            }
        });

        ClusterTbl newCluster = clusterService.find(clusterName);
        logger.info("[updateStorageClusterStatus][getdb]{}, {}", clusterName, newCluster != null ? newCluster.getStatus() : null);
    }

    protected DcTbl findToDc(ClusterTbl cluster, String fromIdc, String toIdc, List<DcTbl> clusterRelatedDc)
            throws ToIdcNotFoundException {
        return findToDc(cluster, fromIdc, toIdc, clusterRelatedDc, null);
    }

    protected DcTbl findToDc(ClusterTbl cluster, String fromIdc, String toIdc, List<DcTbl> clusterRelatedDc,
                             AzGroupClusterEntity azGroupCluster) throws ToIdcNotFoundException {

        DcTbl fromIdcInfo = null;
        for(DcTbl dcTbl : clusterRelatedDc) {
            if(dcTbl.getDcName().equals(fromIdc)) {
                fromIdcInfo = dcTbl;
                break;
            }
        }
        if(StringUtil.isEmpty(toIdc)){
            Map<String, DcTbl> availableDcs = new HashMap<>();
            for (DcTbl dcTbl : clusterRelatedDc) {
                if (!dcTbl.getDcName().equalsIgnoreCase(fromIdc)
                        && isTargetDcAllowed(cluster, fromIdcInfo, dcTbl, azGroupCluster)) {
                    availableDcs.put(dcTbl.getDcName().toUpperCase(), dcTbl);
                }
            }
            if (availableDcs.isEmpty())
                throw new ToIdcNotFoundException(String.format("fromIdc:%s, toIdc empty, no available dcs found from related dcs: %s", fromIdc, clusterRelatedDcToString(clusterRelatedDc)));

            toIdc = dcRelationsService.getClusterTargetDcByPriority(cluster.getId(), cluster.getClusterName(), fromIdc, Lists.newArrayList(availableDcs.keySet()));
            if (StringUtil.isEmpty(toIdc)) {
                logger.error("[findToDc][{}]fromIdc:{}, can not find target dc from available dcs: {}", cluster.getClusterName(), fromIdc, availableDcs);
                throw new ToIdcNotFoundException(String.format("fromIdc:%s, toIdc empty, can not find target dc %s", fromIdc, clusterRelatedDcToString(clusterRelatedDc)));
            }
            return availableDcs.get(toIdc.toUpperCase());
        }else {

            if(toIdc.equalsIgnoreCase(fromIdc)){
                throw new ToIdcNotFoundException(String.format("fromIdc:%s equals with toIdc %s", fromIdc, toIdc));
            }

            for (DcTbl dcTbl : clusterRelatedDc) {
                if (dcTbl.getDcName().equalsIgnoreCase(toIdc)) {
                    if(!isTargetDcAllowed(cluster, fromIdcInfo, dcTbl, azGroupCluster)) {
                        throw new ToIdcNotFoundException(targetDcScopeMismatchMessage(azGroupCluster));
                    }
                    return dcTbl;
                }
            }
            throw new ToIdcNotFoundException(String.format("toIdc : %s, can not find it in all related dcs:%s", toIdc, clusterRelatedDcToString(clusterRelatedDc)));
        }
    }

    private boolean isTargetDcAllowed(ClusterTbl cluster, DcTbl fromIdc, DcTbl toDc,
                                      AzGroupClusterEntity azGroupCluster) {
        if (fromIdc == null || toDc == null) {
            return false;
        }
        if (azGroupCluster != null) {
            return heteroMigrationSupport.isSameAzGroup(cluster.getId(), fromIdc.getDcName(), toDc.getDcName());
        }
        return isSameZone(fromIdc, toDc);
    }

    private String targetDcScopeMismatchMessage(AzGroupClusterEntity azGroupCluster) {
        if (azGroupCluster != null) {
            return "To Idc should be in same az group with from Idc";
        }
        return "To Idc should be in same zone with from Idc";
    }

    protected boolean isSameZone(DcTbl fromIdc, DcTbl toIdc) {
        try {
            return fromIdc.getZoneId() == toIdc.getZoneId();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Set<String> getLatestMigrationOperators(int hours) {
        List<MigrationEventTbl> latestMigrateEvents = migrationEventDao.findLatestMigrateEvent(DateTimeUtils.getHoursBeforeDate(new Date(), hours));
        return latestMigrateEvents.stream().map(migrationEventTbl ->  migrationEventTbl.getOperator()).collect(Collectors.toSet());
    }


    @Override
    public List<MigrationClusterTbl> getLatestMigrationClusters(int seconds) {
        return migrationClusterDao.findLatestMigrationClusterWithClusterName(DateTimeUtils.getSecondBeforeDate(new Date(), seconds));
    }

    @Override
    public List<MigrationClusterTbl> fetchMigrationClusters(Set<String> clusters, long from, long to) {
        return migrationClusterDao.findMigrationClustersByNameAndTime(clusters, new Date(from), new Date(to));
    }

    @Override
    public List<BiMigrationRecord> loadAllBiMigration() {
        List<MigrationBiClusterEntity> entities = migrationBiClusterRepository.selectAll();
        if (entities.isEmpty()) return Collections.emptyList();

        Set<Long> relatedClusterIds = entities.stream().map(MigrationBiClusterEntity::getClusterId).collect(Collectors.toSet());
        List<ClusterEntity> clusterEntities = clusterRepository.selectAllByIds(Lists.newArrayList(relatedClusterIds));
        Map<Long, String> clusterId2Name = new HashMap<>();
        clusterEntities.forEach(cluster -> clusterId2Name.put(cluster.getId(), cluster.getClusterName()));

        List<BiMigrationRecord> records = new ArrayList<>(entities.size());
        for (MigrationBiClusterEntity event: entities) {
            BiMigrationRecord record = BiMigrationRecord.fromMigrationBiClusterEntity(event);
            record.setClusterName(clusterId2Name.get(event.getClusterId()));
            records.add(record);
        }

        return records;
    }

    @Override
    public boolean syncBiMigration(BiMigrationReq biMigrationReq, String operator) throws Exception {
        logger.info("[syncBiMigration][begin]");
        List<ClusterTbl> migrateClusters = biMigrationReq.clusters.stream()
                .filter(cluster -> ClusterType.isSameClusterType(cluster.getClusterType(), ClusterType.BI_DIRECTION))
                .collect(Collectors.toList());
        List<String> excludedDcs = biMigrationReq.excludedDcs.stream().map(DcTbl::getDcName).collect(Collectors.toList());

        List<OuterClientService.ClusterExcludedIdcInfo> clusterExcludedIdcInfos = new ArrayList<>(migrateClusters.size());
        for (ClusterTbl cluster: migrateClusters) {
            OuterClientService.ClusterExcludedIdcInfo info = new OuterClientService.ClusterExcludedIdcInfo();
            info.setClusterName(cluster.getClusterName());
            info.setExcludedDcs(excludedDcs);
            clusterExcludedIdcInfos.add(info);
        }

        boolean rst = OuterClientService.DEFAULT.batchExcludeIdcs(clusterExcludedIdcInfos);
        try {
            Date current = new Date();
            List<MigrationBiClusterEntity> migrationRecord = new ArrayList<>(migrateClusters.size());

            for (ClusterTbl cluster: migrateClusters) {
                MigrationBiClusterEntity entity = new MigrationBiClusterEntity();
                entity.setClusterId(cluster.getId());
                entity.setStatus(rst ? MigrationStatus.TYPE_SUCCESS : MigrationStatus.TYPE_FAIL);
                entity.setOperator(operator);
                entity.setPublishInfo(Codec.DEFAULT.encode(excludedDcs));
                entity.setOperationTime(current);
                migrationRecord.add(entity);
            }

            migrationBiClusterRepository.batchInsert(migrationRecord);
        } catch (Throwable th) {
            logger.info("[syncBiMigration][record fail]", th);
        }
        logger.info("[syncBiMigration][end] {}", rst);
        return rst;
    }

    @Override
    public RetMessage preMigrateSentinelBeacon(MigrationCluster migrationCluster) {
        return migrateSentinelBeaconByDc(migrationCluster.clusterName(), migrationCluster.fromDc(), true);
    }

    @Override
    public RetMessage postMigrateSentinelBeacon(MigrationCluster migrationCluster, Map<String, HostPort> shardMasters) {
        return migrateSentinelBeaconByDc(migrationCluster.clusterName(), migrationCluster.destDc(), false, shardMasters);
    }

    @Override
    public RetMessage preMigrateSentinelBeacon(String clusterName) {
        return migrateSentinelBeaconInCurrentDc(clusterName, true);
    }

    @Override
    public RetMessage postMigrateSentinelBeacon(String clusterName, Map<String, HostPort> shardMasters) {
        return migrateSentinelBeaconInCurrentDc(clusterName, false, shardMasters);
    }

    @Override
    public void postMigrateSentinelBeaconAsync(MigrationCluster migrationCluster, Map<String, HostPort> shardMasters) {
        if (postMigrateBeaconExecutor == null) {
            logger.warn("[postMigrateSentinelBeaconAsync][{}] executor not init", migrationCluster.clusterName());
            return;
        }
        Map<String, HostPort> shardMastersSnapshot = copyShardMasters(shardMasters);
        try {
            postMigrateBeaconExecutor.execute(() -> {
                try {
                    postMigrateSentinelBeacon(migrationCluster, shardMastersSnapshot);
                } catch (Throwable th) {
                    logger.warn("[postMigrateSentinelBeaconAsync][{}] fail", migrationCluster.clusterName(), th);
                }
            });
        } catch (RejectedExecutionException e) {
            // queue full should be dropped
            logger.warn("[postMigrateSentinelBeaconAsync][{}] dropped", migrationCluster.clusterName());
        } catch (Throwable th) {
            logger.warn("[postMigrateSentinelBeaconAsync][{}] submit fail", migrationCluster.clusterName(), th);
        }
    }

    @Override
    public boolean shouldMigrateSentinelBeacon(MigrationCluster migrationCluster) {
        if (migrationCluster == null || StringUtil.isEmpty(migrationCluster.clusterName())) {
            return false;
        }
        String clusterName = migrationCluster.clusterName();
        ClusterMeta clusterMeta = getCurrentDcClusterMeta(clusterName);
        if (clusterMeta == null) {
            return false;
        }
        int orgId = resolveSentinelOrgId(clusterMeta);
        ClusterType clusterType = resolveSentinelClusterType(clusterMeta);
        return isSentinelMigrationEnabled(clusterName, orgId, clusterType);
    }

    private RetMessage migrateSentinelBeaconByDc(String clusterName, String dc, boolean preMigrate) {
        return migrateSentinelBeacon(clusterName, dc, preMigrate, Collections.emptyMap());
    }

    private RetMessage migrateSentinelBeaconByDc(String clusterName, String dc, boolean preMigrate,
                                                 Map<String, HostPort> shardMasters) {
        return migrateSentinelBeacon(clusterName, dc, preMigrate, shardMasters);
    }

    private RetMessage migrateSentinelBeaconInCurrentDc(String clusterName, boolean preMigrate) {
        return migrateSentinelBeacon(clusterName, CURRENT_DC, preMigrate, Collections.emptyMap());
    }

    private RetMessage migrateSentinelBeaconInCurrentDc(String clusterName, boolean preMigrate,
                                                        Map<String, HostPort> shardMasters) {
        return migrateSentinelBeacon(clusterName, CURRENT_DC, preMigrate, shardMasters);
    }

    private RetMessage migrateSentinelBeacon(String clusterName, String dc, boolean preMigrate,
                                             Map<String, HostPort> shardMasters) {
        String targetDc = StringUtil.isEmpty(dc) ? CURRENT_DC : dc;
        ClusterMeta clusterMeta = getClusterMetaInDc(clusterName, targetDc);
        if (clusterMeta == null) {
            return RetMessage.createFailMessage(String.format("cluster %s not found in dc %s meta", clusterName, targetDc));
        }
        int orgId = resolveSentinelOrgId(clusterMeta);
        ClusterType clusterType = resolveSentinelClusterType(clusterMeta);
        if (!isSentinelMigrationEnabled(clusterName, orgId, clusterType)) {
            return RetMessage.createSuccessMessage("sentinel beacon migration skipped");
        }
        if (beaconManager == null) {
            return RetMessage.createFailMessage("BeaconManager unavailable");
        }

        try {
            if (preMigrate) {
                beaconManager.unregisterCluster(clusterName, targetDc, clusterType, orgId, BeaconRouteType.SENTINEL);
                return RetMessage.createSuccessMessage("sentinel beacon unregistered");
            }

            beaconManager.registerCluster(clusterName, targetDc, clusterType, orgId,
                    String.valueOf(System.currentTimeMillis()), BeaconRouteType.SENTINEL, shardMasters);
            return RetMessage.createSuccessMessage("sentinel beacon registered");
        } catch (Throwable th) {
            logger.warn("[migrateSentinelBeacon][{}][{}][{}] fail", clusterName, targetDc, preMigrate, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    private ClusterType resolveSentinelClusterType(ClusterMeta clusterMeta) {
        if (clusterMeta == null) {
            return null;
        }
        if (!StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
            return ClusterType.lookup(clusterMeta.getAzGroupType());
        }
        return ClusterType.lookup(clusterMeta.getType());
    }

    private int resolveSentinelOrgId(ClusterMeta clusterMeta) {
        if (clusterMeta != null && clusterMeta.getOrgId() != null) {
            return clusterMeta.getOrgId();
        }
        return -1;
    }

    private ClusterMeta getCurrentDcClusterMeta(String clusterName) {
        return getClusterMetaInDc(clusterName, CURRENT_DC);
    }

    private ClusterMeta getClusterMetaInDc(String clusterName, String dc) {
        if (metaCache == null || metaCache.getXpipeMeta() == null || metaCache.getXpipeMeta().getDcs() == null
                || !metaCache.getXpipeMeta().getDcs().containsKey(dc)
                || metaCache.getXpipeMeta().getDcs().get(dc).getClusters() == null) {
            return null;
        }
        return metaCache.getXpipeMeta().getDcs().get(dc).getClusters().get(clusterName);
    }

    private boolean isSentinelMigrationEnabled(String clusterName, int orgId, ClusterType clusterType) {
        if (!config.supportSentinelBeacon(orgId, clusterName)) {
            return false;
        }
        return clusterType == ClusterType.ONE_WAY
                || clusterType == ClusterType.SINGLE_DC
                || clusterType == ClusterType.LOCAL_DC;
    }

    private Map<String, HostPort> copyShardMasters(Map<String, HostPort> shardMasters) {
        if (shardMasters == null || shardMasters.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new HashMap<>(shardMasters));
    }

    private String clusterRelatedDcToString(List<DcTbl> clusterRelatedDc) {
        return StringUtil.join(",", (dcTbl) -> dcTbl.getDcName() , clusterRelatedDc);
    }

    @VisibleForTesting
    protected MigrationServiceImpl setAlertManager(AlertManager alertManager) {
        this.alertManager = alertManager;
        return this;
    }

    @VisibleForTesting
    public MigrationServiceImpl setChecker(MigrationSystemAvailableChecker checker) {
        this.checker = checker;
        return this;
    }

    @VisibleForTesting
    public MigrationServiceImpl setMigrationEventManager(MigrationEventManager migrationEventManager) {
        this.migrationEventManager = migrationEventManager;
        return this;
    }

    @VisibleForTesting
    public MigrationEventManager getMigrationEventManager() {
        return migrationEventManager;
    }

    @VisibleForTesting
    public MigrationServiceImpl setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
        return this;
    }

    @VisibleForTesting
    public MigrationServiceImpl setDcClusterService(DcClusterService dcClusterService) {
        this.dcClusterService = dcClusterService;
        return this;
    }

    @VisibleForTesting
    public MigrationServiceImpl setDcService(DcService dcService) {
        this.dcService = dcService;
        return this;
    }

    @VisibleForTesting
    public MigrationServiceImpl setMigrationClusterDao(MigrationClusterDao migrationClusterDao) {
        this.migrationClusterDao = migrationClusterDao;
        return this;
    }

    @VisibleForTesting
    public MigrationServiceImpl setConfigService(ConfigService configService) {
        this.configService = configService;
        return this;
    }

    @VisibleForTesting
    public MigrationServiceImpl setMigrationShardTblDao(MigrationShardTblDao migrationShardTblDao) {
        this.migrationShardTblDao = migrationShardTblDao;
        return this;
    }

    @VisibleForTesting
    void setDcRelationsService(DcRelationsService dcRelationsService) {
        this.dcRelationsService = dcRelationsService;
    }

    @VisibleForTesting
    MigrationServiceImpl setHeteroMigrationSupport(HeteroMigrationSupport heteroMigrationSupport) {
        this.heteroMigrationSupport = heteroMigrationSupport;
        return this;
    }

    @VisibleForTesting
    MigrationServiceImpl setDcCache(DcCache dcCache) {
        this.dcCache = dcCache;
        return this;
    }

    @VisibleForTesting
    MigrationServiceImpl setMigrationEventDao(MigrationEventDao migrationEventDao) {
        this.migrationEventDao = migrationEventDao;
        return this;
    }
}
