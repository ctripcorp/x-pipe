package com.ctrip.xpipe.redis.meta.server.dao;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.dao.DaoException;
import com.ctrip.xpipe.redis.core.dao.MetaDao;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public abstract class AbstractMetaServerDao extends AbstractLifecycle implements MetaServerDao{
	
	protected MetaDao metaDao;
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected String currentDc = FoundationService.DEFAULT.getDataCenter();

	
	
	@Override
	protected void doInitialize() throws Exception {
		
		metaDao = loadMetaDao();
	}

	protected abstract MetaDao loadMetaDao();

	
	@Override
	public ClusterMeta getClusterMeta(String clusterId) {
		
		return metaDao.getClusterMeta(currentDc, clusterId);
	}

	@Override
	public ShardMeta getShardMeta(String clusterId, String shardId) {
		return metaDao.getShardMeta(shardId, clusterId, shardId);
	}

	@Override
	public List<KeeperMeta> getKeepers(String clusterId, String shardId) {
		return metaDao.getKeepers(currentDc, clusterId, shardId);
	}

	@Override
	public List<RedisMeta> getRedises(String clusterId, String shardId) {
		return metaDao.getRedises(currentDc, clusterId, shardId);
	}

	@Override
	public KeeperMeta getKeeperActive(String clusterId, String shardId) {
		return metaDao.getKeeperActive(currentDc, clusterId, shardId);
	}

	@Override
	public List<KeeperMeta> getKeeperBackup(String clusterId, String shardId) {
		return metaDao.getKeeperBackup(currentDc, clusterId, shardId);
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		
		for(RedisMeta redisMeta : getRedises(clusterId, shardId)){
			if(redisMeta.isMaster()){
				return redisMeta;
			}
		}
		
		return null;
	}

	@Override
	public List<MetaServerMeta> getMetaServers() {
		return metaDao.getMetaServers(currentDc);
	}

	@Override
	public ZkServerMeta getZkServerMeta() {
		return metaDao.getZkServerMeta(currentDc);
	}

	@Override
	public boolean updateKeeperActive(String clusterId, String shardId, KeeperMeta activeKeeper) throws DaoException {
		
		return metaDao.updateKeeperActive(currentDc, clusterId, shardId, activeKeeper);
	}

	@Override
	public boolean updateRedisMaster(String clusterId, String shardId, RedisMeta redisMaster) throws DaoException {
		
		return metaDao.updateRedisMaster(currentDc, clusterId, shardId, redisMaster);
	}

	@Override
	public Set<String> getClusters() {
		return metaDao.getDcClusters(currentDc);
	}

	@Override
	public boolean updateUpstreamKeeper(String clusterId, String shardId, String address) throws DaoException {
		return metaDao.updateUpstreamKeeper(currentDc, clusterId, shardId, address);
	}

	@Override
	public String getUpstream(String clusterId, String shardId) throws DaoException {
		return metaDao.getUpstream(currentDc, clusterId, shardId);
	}
}
