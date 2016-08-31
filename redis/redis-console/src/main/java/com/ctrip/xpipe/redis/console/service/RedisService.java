package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;

/**
 * @author shyin
 * 
 * Aug 20, 2016
 */
@Service
public class RedisService extends AbstractConsoleService<RedisTblDao>{


    public List<RedisTbl> findByDcClusterShardId(final long dcClusterShardId){
    	return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
			@Override
			public List<RedisTbl> doQuery() throws DalException {
				return dao.findAllByDcClusterShardId(dcClusterShardId, RedisTblEntity.READSET_FULL);
			}
    	});
    }
    
    public RedisTbl load(final long id) {
    	return queryHandler.handleQuery(new DalQuery<RedisTbl>() {
			@Override
			public RedisTbl doQuery() throws DalException {
				return dao.findByPK(id, RedisTblEntity.READSET_FULL);
			}
    	});
    }

    public RedisTbl findActiveKeeper(List<RedisTbl> redises) {
    	RedisTbl result = null;
    	for(RedisTbl redis : redises) {
    		if(redis.getRedisRole().equals("keeper") && (redis.isKeeperActive() == true )) {
    			result = redis;
    			break;
    		}
    	}
    	return result;
    }
    
    public RedisTbl createRedis(final RedisTbl redis) {
		redis.setId(0);
		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.insert(redis);
			}
		});

		List<RedisTbl> redisTbls =
				queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
					@Override
					public List<RedisTbl> doQuery() throws DalException {
						return dao.findAllByDcClusterShardId(redis.getDcClusterShardId(), RedisTblEntity.READSET_FULL);
					}
				});

		return redisTbls.get(redisTbls.size() - 1);
    }
    
    public void deleteRedis(final String redisName) {
		final RedisTbl toDeleteRedis = queryHandler.handleQuery(new DalQuery<RedisTbl>() {
			@Override
			public RedisTbl doQuery() throws DalException {
				return dao.findByName(redisName, RedisTblEntity.READSET_FULL);
			}
		});

		toDeleteRedis.setDeleted(true);

		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(toDeleteRedis, RedisTblEntity.UPDATESET_FULL);
			}
		});
    }
    
}
