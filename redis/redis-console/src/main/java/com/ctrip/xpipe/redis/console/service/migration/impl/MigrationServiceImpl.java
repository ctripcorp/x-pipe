package com.ctrip.xpipe.redis.console.service.migration.impl;

import java.rmi.ServerException;
import java.util.List;

import javax.annotation.PostConstruct;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
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
public class MigrationServiceImpl extends AbstractConsoleService<MigrationEventTblDao> implements MigrationService{

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
	};

	@Override
	public MigrationClusterTbl findMigrationCluster(final long eventId, final long clusterId) {

		return migrationClusterDao.findByEventIdAndClusterId(eventId, clusterId);
	}
	
	@Override
	public MigrationClusterTbl findLatestUnfinishedMigrationCluster(final long clusterId) {


		List<MigrationClusterTbl> unfinishedByClusterId = migrationClusterDao.findUnfinishedByClusterId(clusterId);

		if(unfinishedByClusterId.size() == 0){
			return null;
		}

		if(unfinishedByClusterId.size() > 1){
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
	public void updateMigrationShard(final MigrationShardTbl shard) {
		queryHandler.handleQuery(new DalQuery<Void>() {
			@Override
			public Void doQuery() throws DalException {
				migrationShardTblDao.updateByPK(shard, MigrationShardTblEntity.UPDATESET_FULL);
				return null;
			}
		});
	}
	
	@Override
	public void updateMigrationCluster(final MigrationClusterTbl cluster) {
		migrationClusterDao.updateByPK(cluster);
	}

	@Override
	public void continueMigrationCluster(final long eventId, final long clusterId) {
		if(isMigrationClusterExist(eventId, clusterId)) {
			migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).process();
		}
	}
	
	@Override
	public void continueMigrationEvent(long id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cancelMigrationCluster(long eventId, long clusterId) {
		if(isMigrationClusterExist(eventId, clusterId)) {
			migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).cancel();
		}
	}

	@Override
	public void rollbackMigrationCluster(long eventId, long clusterId) {
		if(isMigrationClusterExist(eventId, clusterId)) {
			migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).rollback();
		}
	}

	@Override
	public void forcePublishMigrationCluster(long eventId, long clusterId) {
		if(isMigrationClusterExist(eventId, clusterId)) {
			migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).forcePublish();
		}
	}

	@Override
	public void forceEndMigrationClsuter(long eventId, long clusterId) {
		if(isMigrationClusterExist(eventId, clusterId)) {
			migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId).forceEnd();
		}
	}
	
	private boolean isMigrationClusterExist(long eventId, long clusterId) {
		boolean ret = false;
		if(null != migrationEventManager.getEvent(eventId)) {
			if(null != migrationEventManager.getEvent(eventId).getMigrationCluster(clusterId)) {
				ret = true;
			}
		}
		return ret;
	}

	@Override
	public TryMigrateResult tryMigrate(String clusterName, String fromIdc) throws ClusterNotFoundException, ClusterActiveDcNotRequest, ClusterMigratingNow {

		ClusterTbl clusterTbl  = clusterService.find(clusterName);
		if(clusterTbl == null){
			throw new ClusterNotFoundException(clusterName);
		}

		MigrationClusterTbl unfinished = findLatestUnfinishedMigrationCluster(clusterTbl.getId());
		if(unfinished != null){
			long fromDcId = unfinished.getSourceDcId();
			long toDcId = unfinished.getDestinationDcId();
			throw new ClusterMigratingNow(clusterName, dcService.getDcName(fromDcId), dcService.getDcName(toDcId), unfinished.getMigrationEventId());
		}

		long activedcId = clusterTbl.getActivedcId();
		DcTbl activeDc = dcService.find(activedcId);
		if(fromIdc != null && !fromIdc.equalsIgnoreCase(activeDc.getDcName())){
			throw new ClusterActiveDcNotRequest(clusterName, fromIdc, activeDc.getDcName());
		}

		List<DcTbl> clusterRelatedDc = dcService.findClusterRelatedDc(clusterName);
		logger.debug("[tryMigrate][clusterRelatedDc]", clusterRelatedDc);

		DcTbl toDc = findToDc(fromIdc, clusterRelatedDc);
		return new TryMigrateResult(clusterTbl, activeDc, toDc);
	}

	private DcTbl findToDc(String fromIdc, List<DcTbl> clusterRelatedDc) {

		//simple
		for(DcTbl dcTbl : clusterRelatedDc){
			if(!dcTbl.getDcName().equalsIgnoreCase(fromIdc)){
				return dcTbl;
			}
		}
		throw new IllegalStateException("can not find target dc " + fromIdc + "," + clusterRelatedDc);
	}

}
