package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;

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
    
    public void createRedis(RedisTbl redis) {
    	// TODO
    }
    
    public void deleteRedis(String redisName) {
    	// TODO
    }
    
    public void deleteRedisBatch(List<RedisTbl> redises) {
    	if(null == redises) {
    		return ;
    	}
    	
    	for(RedisTbl redis : redises) {
    		RedisTbl proto = redis;
    		proto.setRedisName(DataModifiedTimeGenerator.generateModifiedTime() + "-" + redis.getRedisName());
    		proto.setDeleted(true);
    	}
    	
    	try {
			dao.deleteBatch((RedisTbl[])redises.toArray(),RedisTblEntity.UPDATESET_FULL);
		} catch (DalException e) {
			throw new BadRequestException("Redises cannot be deleted.");
		}
    }
    
}
