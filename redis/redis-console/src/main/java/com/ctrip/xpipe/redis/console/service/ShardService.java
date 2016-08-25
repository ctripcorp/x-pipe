package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import java.util.List;

/**
 * @author shyin
 *
 * Aug 20, 2016
 */
@Service
public class ShardService extends AbstractConsoleService<ShardTblDao>{
	@Autowired
	private ShardDao shardDao;
	
    public ShardTbl load(final long shardId){
    	return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return dao.findByPK(shardId, ShardTblEntity.READSET_FULL);
			}
    	});
    }
    
    public ShardTbl load(final String clusterName, final String shardName) {
    	return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return dao.findShard(clusterName, shardName, ShardTblEntity.READSET_FULL);
			}
    	});
    }

    public List<ShardTbl> loadAllByClusterName(final String clusterName){
    	return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return dao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
			}
    	});
    }
    
    public List<ShardTbl> findAllShardNamesByClusterName(final String clusterName) {
    	return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return dao.findAllByClusterName(clusterName, ShardTblEntity.READSET_NAME);
			}
    	});
    }
    
    public ShardTbl createShard(final String clusterName, final ShardTbl shard) {
    	return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return shardDao.createShard(clusterName, shard);
			}
    	});
    }
    
    public void deleteShards(String clusterName, String shardName) {
    	// TODO
    	/** Delete shard-info together with dc-clsuter-shard && redises under d-c-s **/
    }
    
    public void deleteShardsBatch(List<ShardTbl> shards) {
    	// TODO
    }
    
    
}
