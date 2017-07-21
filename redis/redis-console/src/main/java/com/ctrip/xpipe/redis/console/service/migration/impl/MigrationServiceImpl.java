package com.ctrip.xpipe.redis.console.service.migration.impl;

import java.rmi.ServerException;
import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterActiveDcNotRequest;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterMigratingNow;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;

@Service
public class MigrationServiceImpl extends AbstractConsoleService<MigrationEventTblDao> implements MigrationService {

    @Autowired
    private MigrationEventDao migrationEventDao;

    @Autowired
    private MigrationEventManager migrationEventManager;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private MigrationClusterDao migrationClusterDao;
    private MigrationShardTblDao migrationShardTblDao;

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
    public List<MigrationEventTbl> findAll() {
        return queryHandler.handleQuery(new DalQuery<List<MigrationEventTbl>>() {
            @Override
            public List<MigrationEventTbl> doQuery() throws DalException {
                return dao.findAll(MigrationEventTblEntity.READSET_FULL);
            }
        });
    }

    ;

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
            EventMonitor.DEFAULT.logAlertEvent(String.format("[unfinished > 1]%d : %d", unfinishedByClusterId.size(), clusterId));
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
        MigrationEvent event = migrationEventDao.createMigrationEvent(request);
        migrationEventManager.addEvent(event);
        return event.getEvent().getId();
    }

    @Override
    public void updateMigrationShardLogById(final long id, final String log) {

        MigrationShardTbl migrationShardTbl = new MigrationShardTbl();
        migrationShardTbl.setId(id);
        migrationShardTbl.setLog(log);

        queryHandler.handleQuery(new DalQuery<Integer>() {
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
    public void forcePublishMigrationCluster(long eventId, long clusterId) {
        if (isMigrationClusterExist(eventId, clusterId)) {
            migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).forcePublish();
        }
    }

    @Override
    public void forceEndMigrationClsuter(long eventId, long clusterId) {
        if (isMigrationClusterExist(eventId, clusterId)) {
            migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).forceEnd();
        }
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
    public TryMigrateResult tryMigrate(String clusterName, String fromIdc) throws ClusterNotFoundException, ClusterActiveDcNotRequest, ClusterMigratingNow {

        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            throw new ClusterNotFoundException(clusterName);
        }

        MigrationClusterTbl unfinished = findLatestUnfinishedMigrationCluster(clusterTbl.getId());
        if (unfinished != null) {
            long fromDcId = unfinished.getSourceDcId();
            long toDcId = unfinished.getDestinationDcId();
            throw new ClusterMigratingNow(clusterName, dcService.getDcName(fromDcId), dcService.getDcName(toDcId), unfinished.getMigrationEventId());
        }

        long activedcId = clusterTbl.getActivedcId();
        DcTbl activeDc = dcService.find(activedcId);
        if (fromIdc != null && !fromIdc.equalsIgnoreCase(activeDc.getDcName())) {
            throw new ClusterActiveDcNotRequest(clusterName, fromIdc, activeDc.getDcName());
        }

        List<DcTbl> clusterRelatedDc = dcService.findClusterRelatedDc(clusterName);
        logger.debug("[tryMigrate][clusterRelatedDc]", clusterRelatedDc);

        DcTbl toDc = findToDc(fromIdc, clusterRelatedDc);
        return new TryMigrateResult(clusterTbl, activeDc, toDc);
    }

    private DcTbl findToDc(String fromIdc, List<DcTbl> clusterRelatedDc) {

        //simple
        for (DcTbl dcTbl : clusterRelatedDc) {
            if (!dcTbl.getDcName().equalsIgnoreCase(fromIdc)) {
                return dcTbl;
            }
        }
        throw new IllegalStateException("can not find target dc " + fromIdc + "," + clusterRelatedDc);
    }

}
