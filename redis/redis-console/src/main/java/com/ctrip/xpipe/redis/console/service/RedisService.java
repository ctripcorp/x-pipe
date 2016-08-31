package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.enums.RedisRole;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.google.common.base.Strings;

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
public class RedisService extends AbstractConsoleService<RedisTblDao>{
	@Autowired
	DcClusterShardService dcClusterShardService;
	@Autowired
	RedisDao redisDao;

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
    
    // TODO
    public RedisTbl bindRedis(final String clusterName, final String dcName, final String shardName, RedisTbl redis) {
    	DcClusterShardTbl dcClusterShard = dcClusterShardService.load(dcName, clusterName, shardName);
    	validateRedis(redis);
    	
    	if("redis".equals(redis.getRedisRole())) {
    		return createRedis(dcClusterShard, redis);
    	}
    	if ("keeper".equals(redis.getRedisRole())) {
    		return createKeeper(dcClusterShard, redis);
    	}
    	
    	throw new BadRequestException("Invalid redis role");
    }
    
    // TODO
    private RedisTbl createRedis(DcClusterShardTbl dcClusterShard, RedisTbl redis) {
    	RedisTbl proto = dao.createLocal();
    	proto.setDcClusterShardId(dcClusterShard.getDcClusterShardId())
    			.setRedisIp(redis.getRedisIp())
    			.setRedisPort(redis.getRedisPort())
    			.setRedisRole(redis.getRedisRole());
    	// redis master check
    	 
    	
    	return null;
    }
    
    // TODO
    private RedisTbl createKeeper(DcClusterShardTbl dcClusterShard, RedisTbl redis) {
    	return null;
    }
    
    // TODO
    private void validateRedis(RedisTbl redis) {
    	if(redis.getRedisPort() <= 0 || Strings.isNullOrEmpty(redis.getRedisIp()) || !isValidRedisRole(redis.getRedisRole())) {
    		throw new BadRequestException("Invalid redis configuration.");
    	}
    }
    
    private boolean isValidRedisRole(String type) {
        return RedisRole.KEEPER.getValue().equals(type) || RedisRole.REDIS.getValue().equals(type);
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

}
