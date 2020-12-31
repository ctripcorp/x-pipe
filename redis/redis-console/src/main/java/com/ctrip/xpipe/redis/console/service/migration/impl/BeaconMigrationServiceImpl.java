package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.migration.BeaconMetaComparator;
import com.ctrip.xpipe.redis.console.service.migration.BeaconMigrationService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * @author lishanglin
 * date 2020/12/28
 */
@Service
public class BeaconMigrationServiceImpl implements BeaconMigrationService {

    private MigrationSystemAvailableChecker checker;

    private MigrationEventManager migrationEventManager;

    private ConfigService configService;

    private ClusterService clusterService;

    private MigrationEventDao migrationEventDao;

    private MigrationClusterDao migrationClusterDao;

    private BeaconMetaComparator metaComparator;

    private DcService dcService;

    private static final String MIGRATION_OPERATOR = "Beacon";

    private Logger logger = LoggerFactory.getLogger(BeaconMigrationServiceImpl.class);

    @Autowired
    public BeaconMigrationServiceImpl(MigrationSystemAvailableChecker checker, MigrationEventManager migrationEventManager,
                                      ConfigService configService, DcService dcService, ClusterService clusterService,
                                      MigrationEventDao migrationEventDao, MigrationClusterDao migrationClusterDao,
                                      BeaconMetaComparator metaComparator) {
        this.checker = checker;
        this.migrationEventManager = migrationEventManager;
        this.configService = configService;
        this.dcService = dcService;
        this.clusterService = clusterService;
        this.migrationEventDao = migrationEventDao;
        this.migrationClusterDao = migrationClusterDao;
        this.metaComparator = metaComparator;
    }

    @Override
    public long buildMigration(BeaconMigrationRequest migrationRequest) throws ClusterNotFoundException, WrongClusterMetaException,
            NoAvailableDcException, MigrationNotSupportException, MigrationSystemNotHealthyException,
            MigrationNoNeedException, UnknownTargetDcException, MigrationConflictException {

        if(!checker.getResult().isAvaiable() && !configService.ignoreMigrationSystemAvailability()) {
            throw new MigrationSystemNotHealthyException(checker.getResult().getMessage());
        }

        String clusterName = migrationRequest.getClusterName();
        ClusterTbl clusterTbl = tryGetMigrationCluster(clusterName);
        migrationRequest.setClusterId(clusterTbl.getId());

        // check cluster meta consistent
        if (!metaComparator.compareWithXPipe(clusterName, migrationRequest.getGroups())) {
            throw new WrongClusterMetaException(clusterName);
        }

        DcTbl activeDcTbl = dcService.find(clusterTbl.getActivedcId());
        Set<String> failDcs = migrationRequest.getFailDcs();
        if (!failDcs.contains(activeDcTbl.getDcName())) {
            throw new MigrationNoNeedException(clusterName);
        }

        DcTbl unfinishedTargetIdc = tryGetUnfinishedTargetDc(clusterTbl.getId(), clusterTbl.getMigrationEventId());
        if (null != unfinishedTargetIdc) {
            checkMigrationConflict(unfinishedTargetIdc, migrationRequest);

            logger.info("[buildMigration][{}] reuse unfinished event {}", clusterName, clusterTbl.getMigrationEventId());
            return migrationEventManager.getEvent(clusterTbl.getMigrationEventId()).getMigrationEventId();
        } else {
            DcTbl targetDcTbl = tryChooseTargetDc(activeDcTbl, migrationRequest);

            logger.info("[buildMigration][{}] create new event", clusterName);
            MigrationEvent event = migrationEventDao.createMigrationEvent(buildXPipeMigrationRequest(clusterTbl, activeDcTbl, targetDcTbl));
            migrationEventManager.addEvent(event);
            return event.getEvent().getId();
        }
    }

    @Override
    public CommandFuture<Boolean> doMigration(long eventId, long clusterId) throws Exception {
        MigrationEvent event = migrationEventManager.getEvent(eventId);
        CommandFuture<Boolean> migrationFuture = new DefaultCommandFuture<>();

        event.getMigrationCluster(clusterId).addObserver((args, observable) -> {
            MigrationCluster migrationCluster = event.getMigrationCluster(clusterId);
            MigrationStatus migrationStatus = migrationCluster.getStatus();

            if (!migrationCluster.isStarted() || migrationStatus.isTerminated()) {
                if (MigrationStatus.Success.equals(migrationStatus)) {
                    migrationFuture.setSuccess(true);
                } else {
                    migrationFuture.setFailure(new ClusterMigrationNotSuccessException(migrationCluster.clusterName(), migrationStatus));
                }
            }
        });
        event.processCluster(clusterId);

        return migrationFuture;
    }

    private ClusterTbl tryGetMigrationCluster(String clusterName) throws MigrationNotSupportException, ClusterNotFoundException {
        // cluster exist
        if (StringUtil.isEmpty(clusterName)) {
            throw new ClusterNotFoundException(clusterName);
        }
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (null == clusterTbl) {
            throw new ClusterNotFoundException(clusterName);
        }
        // cluster type support migration
        if (!ClusterType.lookup(clusterTbl.getClusterType()).supportMigration()) {
            throw new MigrationNotSupportException(clusterName);
        }

        return clusterTbl;
    }

    private DcTbl tryGetUnfinishedTargetDc(long clusterId, long unfinishedEventId) {
        if (unfinishedEventId <= 0) return null;

        MigrationClusterTbl unfinishedMigration = migrationClusterDao.findByEventIdAndClusterId(unfinishedEventId, clusterId);
        String status = unfinishedMigration.getStatus();
        if (!MigrationStatus.TYPE_SUCCESS.equals(status) && !MigrationStatus.TYPE_FAIL.equals(status)) {
            return dcService.find(unfinishedMigration.getDestinationDcId());
        }

        return null;
    }

    private DcTbl tryChooseTargetDc(DcTbl currentDc, BeaconMigrationRequest migrationRequest)
            throws NoAvailableDcException, MigrationNoNeedException, UnknownTargetDcException {
        Boolean forced = migrationRequest.getForced();
        String clusterName = migrationRequest.getClusterName();

        if (null == forced || !forced || StringUtil.isEmpty(migrationRequest.getTargetIDC())) {
            Set<String> availableDcs = migrationRequest.getAvailableDcs();
            if (null == availableDcs || availableDcs.isEmpty()) {
                throw new NoAvailableDcException(clusterName);
            }
            if (availableDcs.contains(currentDc.getDcName())) {
                throw new MigrationNoNeedException(clusterName);
            }
            migrationRequest.setTargetIDC(availableDcs.iterator().next());
        }

        DcTbl targetDcTbl = dcService.findByDcName(migrationRequest.getTargetIDC());
        if (null == targetDcTbl) throw new UnknownTargetDcException(clusterName, migrationRequest.getTargetIDC());
        if (currentDc.getId() == targetDcTbl.getId()) throw new MigrationNoNeedException(clusterName);

        return targetDcTbl;
    }

    private void checkMigrationConflict(DcTbl unfinishedTargetDc, BeaconMigrationRequest migrationRequest) throws MigrationConflictException {
        Set<String> availableDcs = migrationRequest.getAvailableDcs();
        if (!availableDcs.contains(unfinishedTargetDc.getDcName())) {
            throw new MigrationConflictException(migrationRequest.getClusterName(), availableDcs.iterator().next(), unfinishedTargetDc.getDcName());
        }
    }

    private MigrationRequest buildXPipeMigrationRequest(ClusterTbl cluster, DcTbl currentDcTbl, DcTbl targetDcTbl) {
        MigrationRequest request = new MigrationRequest(MIGRATION_OPERATOR);
        request.setTag(MIGRATION_OPERATOR);
        MigrationRequest.ClusterInfo migrationCluster = new MigrationRequest.ClusterInfo(cluster.getId(), cluster.getClusterName(),
                currentDcTbl.getId(), currentDcTbl.getDcName(), targetDcTbl.getId(), targetDcTbl.getDcName());
        request.addClusterInfo(migrationCluster);

        return request;
    }

}
