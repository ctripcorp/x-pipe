package com.ctrip.xpipe.redis.console.dao;

import javax.annotation.PostConstruct;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationShardTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;

@Repository
public class MigrationShardDao extends AbstractXpipeConsoleDAO {
	
	private MigrationShardTblDao migrationShardDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			migrationShardDao = ContainerLoader.getDefaultContainer().lookup(MigrationShardTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	public void updateMigrationShard(MigrationShardTbl migrationShard) {
		MigrationShardTbl proto = migrationShardDao.createLocal();
		proto.setId(migrationShard.getId()).setMigrationClusterId(migrationShard.getMigrationClusterId())
			.setShardId(migrationShard.getShardId()).setLog(migrationShard.getLog());
		
		final MigrationShardTbl forUpdate = proto;
		queryHandler.handleQuery(new DalQuery<Void>() {
			@Override
			public Void doQuery() throws DalException {
				migrationShardDao.updateByPK(forUpdate, MigrationShardTblEntity.UPDATESET_FULL);
				return null;
			}
		});
	}

}
