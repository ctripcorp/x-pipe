package com.ctrip.xpipe.redis.console.service.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Resource;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.pool.XpipeKeyedObjectPool;
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
	private XpipeKeyedObjectPool<String, CuratorFramework> zkPool;
	
	public DefaultMetaService() {
	}
	
	@Override
	public boolean updateKeeperActive(String dc, final String clusterId, final String shardId, final KeeperMeta activeKeeper) throws DaoException {
		
		boolean activeChanged = metaDao.updateKeeperActive(dc, clusterId, shardId, activeKeeper);
		
		if(!activeChanged){
			logger.info("[updateKeeperActive][not changed]{},{},{},{}", dc, clusterId, shardId, activeKeeper);
			return false;
		}
		
		logger.info("[updateKeeperActive][changed]{},{},{},{}", dc, clusterId, shardId, activeKeeper);
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
		
		logger.info("[updateRedisMaster][changed]{},{},{},{}", dc, clusterId, shardId, redisMaster);
		notifyDcs(clusterId, shardId);
		return activeChanged;
	}

	@Override
	public boolean updateActiveDc(String clusterId, String activeDc) throws DaoException {
		
		boolean activeDcChanged = metaDao.updateActiveDc(clusterId, activeDc);
		
		if(!activeDcChanged){
			logger.info("[updateActiveDc][not changed]{},{}", clusterId, activeDc);
			return false;
		}
		
		logger.info("[updateActiveDc][changed notify dcs]{},{}", clusterId, activeDc);
		notifyDcs(clusterId, null);
		return activeDcChanged;
	}

	
	private void notifyDcs(final String clusterId, final String shardId) {
		
		for(final String newDc : metaDao.getDcs()){
			
			executors.execute(new Runnable() {
				
				@Override
				public void run() {
					notifyDc(newDc);
				}
			});
		}
	}

	private void notifyDc(String dc){

		String zkAddress = null;
		CuratorFramework curatorFramework = null;
		try {
			zkAddress = metaDao.getZkServerMeta(dc).getAddress();
			curatorFramework = zkPool.borrowObject(zkAddress);
			logger.info("[notifyDc]{}, {}", dc, zkAddress);
			//TODO split into small pieces
			new DefaultMetaOperation(curatorFramework).update(metaDao.getXpipeMeta());
		} catch (Exception e) {
			logger.error("[notifyDc]" + dc, e);
		}finally{
			if(curatorFramework != null){
				try {
					zkPool.returnObject(zkAddress, curatorFramework);
				} catch (Exception e) {
					logger.error("[return object exception]" + zkAddress + ","+ curatorFramework, e);
				}
			}
		}
	}

}
