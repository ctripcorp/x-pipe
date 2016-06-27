package com.ctrip.xpipe.redis.console.service.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Resource;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.console.dao.DaoException;
import com.ctrip.xpipe.redis.console.dao.MetaDao;
import com.ctrip.xpipe.redis.console.service.MetaService;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultMetaOperation;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@Component
public class DefaultMetaService implements MetaService{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private MetaDao metaDao;
	
	private ExecutorService executors  = Executors.newCachedThreadPool(); 
	
	@Resource(name="zkPool")
	private KeyedObjectPool<String, CuratorFramework> zkPool;
	
	public DefaultMetaService() {
	}
	
	@Override
	public boolean updateKeeperActive(String dc, final String clusterId, final String shardId, final KeeperMeta activeKeeper) throws DaoException {
		
		boolean activeChanged = metaDao.updateKeeperActive(dc, clusterId, shardId, activeKeeper);
		
		if(!activeChanged){
			logger.info("[updateKeeperActive][not changed]{},{},{},{}", dc, clusterId, shardId, activeKeeper);
			return false;
		}
		
		notifyDcs(clusterId, shardId);
		
		return activeChanged;
	}


	@Override
	public boolean updateRedisMaster(String dc, String clusterId, String shardId, RedisMeta redisMaster)
			throws DaoException {
		
		boolean activeChanged = metaDao.updateRedisMaster(dc, clusterId, shardId, redisMaster);
		
		if(!activeChanged){
			logger.info("[updateRedisMaster][not changed]{},{},{},{}", dc, clusterId, shardId, redisMaster);
			return false;
		}
		
		notifyDcs(clusterId, shardId);
		return activeChanged;
	}

	private void notifyDcs(final String clusterId, final String shardId) {
		
		for(final String newDc : metaDao.getDcs()){
			
			executors.execute(new Runnable() {
				
				@Override
				public void run() {
					notifyDc(newDc, clusterId, shardId);
				}
			});
		}
	}

	private void notifyDc(String dc, String clusterId, String shardId){
		
		try {
			logger.info("[notifyDc]{}, {}, {}, {}", dc, clusterId, shardId);
			String zkAddress = metaDao.getZkServerMeta(dc).getAddress();
			logger.info("[notifyDc]{}", zkAddress);
			CuratorFramework curatorFramework = zkPool.borrowObject(zkAddress);
			//TODO split into small pieces
			new DefaultMetaOperation(curatorFramework).update(metaDao.getXpipeMeta());
		} catch (Exception e) {
			logger.error("[notifyDc]" + dc, e);
		}
	}
}
