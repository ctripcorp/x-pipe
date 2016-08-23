package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;

import java.util.List;

@Service
public class RedisService {
    private RedisTblDao redisTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            redisTblDao = ContainerLoader.getDefaultContainer().lookup(RedisTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }

    public List<RedisTbl> findByDcClusterShardId(long dcClusterShardId){
        try {
            return redisTblDao.findAllByDcClusterShardId(dcClusterShardId, RedisTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Redises not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load redises failed.", e);
        }
    }
    
    public RedisTbl load(long id) {
    	try {
			return redisTblDao.findByPK(id, RedisTblEntity.READSET_FULL);
		} catch (DalNotFoundException e) {
            throw new DataNotFoundException("Redise not found by id.", e);
        } catch (DalException e) {
            throw new ServerException("Load redise failed.", e);
        }
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
    
    @Transactional
    public void createRedis(RedisTbl redis) {
    	/** TODO **/
    }
    
    @Transactional
    public void deleteRedis(String redisName) {
    	/** TODO **/
    }
    
    @Transactional
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
			redisTblDao.deleteBatch((RedisTbl[])redises.toArray(),RedisTblEntity.UPDATESET_FULL);
		} catch (DalException e) {
			throw new BadRequestException("Redises cannot be deleted.");
		}
    }
    
}
