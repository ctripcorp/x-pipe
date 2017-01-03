package com.ctrip.xpipe.redis.console.service.migration.impl;

import java.rmi.ServerException;
import java.util.List;

import javax.annotation.PostConstruct;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.model.MigrationClusterModel;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.MigrationEventModel;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationEventTblEntity;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationShardTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;

@Service
public class MigrationServiceImpl extends AbstractConsoleService<MigrationEventTblDao> implements MigrationService{

	@Autowired
	private MigrationEventDao migrationEventDao;
	@Autowired
	private MigrationEventManager migrationEventManager;
	
	private MigrationClusterTblDao migrationClusterDao;
	private MigrationShardTblDao migrationShardTblDao;
	
	@PostConstruct
	private void postConstruct() throws ServerException {
		try {
			migrationClusterDao = ContainerLoader.getDefaultContainer().lookup(MigrationClusterTblDao.class);
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
		return queryHandler.handleQuery(new DalQuery<MigrationClusterTbl>() {
			@Override
			public MigrationClusterTbl doQuery() throws DalException {
				return migrationClusterDao.findByEventIdAndClusterId(eventId, clusterId, MigrationClusterTblEntity.READSET_FULL);
			}
		});
	}
	
	@Override
	public List<MigrationClusterTbl> findAllMigrationCluster(final long clusterId) {
		return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
			@Override
			public List<MigrationClusterTbl> doQuery() throws DalException {
				return migrationClusterDao.findAllByClusterId(clusterId, MigrationClusterTblEntity.READSET_FULL);
			}
		});
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
	public Long createMigrationEvent(MigrationEventModel events) {
		return migrationEventDao.createMigrationEvnet(events);
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
		queryHandler.handleQuery(new DalQuery<Void>() {
			@Override
			public Void doQuery() throws DalException {
				migrationClusterDao.updateByPK(cluster, MigrationClusterTblEntity.UPDATESET_FULL);
				return null;
			}
		});
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

}
