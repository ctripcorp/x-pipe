package com.ctrip.xpipe.redis.meta.server.service;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.dao.DaoException;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperSetStateCommand;
import com.ctrip.xpipe.redis.meta.server.dao.MetaServerDao;
import com.ctrip.xpipe.redis.meta.server.dao.MetaServerUpdateOperation;
import com.ctrip.xpipe.redis.meta.server.exception.RedisMetaServerException;
import com.ctrip.xpipe.redis.meta.server.impl.MetaChangeListener;
import com.ctrip.xpipe.redis.meta.server.impl.event.ActiveKeeperChanged;
import com.ctrip.xpipe.redis.meta.server.impl.event.RedisMasterChanged;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.job.SlavePromotionJob;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Jul 8, 2016
 */
@Component
public class MetaServerService extends AbstractLifecycleObservable implements MetaServerUpdateOperation{
	
	protected static Logger logger = LoggerFactory.getLogger(MetaServerService.class);
	
	@Resource( name = "clientPool" )
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	
	private ExecutorService executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("META-SERVICE"));
	
	@Autowired
	private List<MetaChangeListener>  metaChangeListeners;

	@Autowired
	private MetaServerDao metaServerDao;
	
	
	@Override
	protected void doStart() throws Exception {
		
		for(MetaChangeListener metaChangeListener : metaChangeListeners){
			logger.info("[doStart][addObserver]{}", metaChangeListener);
			addObserver(metaChangeListener);
		}
	}
	
	
	@Override
	public boolean updateKeeperActive(String clusterId, String shardId, KeeperMeta activeKeeper) throws Exception {
		
		logger.info("[updateKeeperActive]{},{},{}", clusterId, shardId, activeKeeper);
		
		KeeperMeta oldActiveKeeper = metaServerDao.getKeeperActive(clusterId, shardId);
		
		if(!metaServerDao.updateKeeperActive(clusterId, shardId, activeKeeper)){
			logger.info("[updateKeeperActive][keeper active not changed]");
			return false;
		}
		//make sure keeper in proper state
		List<KeeperMeta> keepers = metaServerDao.getKeepers(clusterId, shardId);
		InetSocketAddress activeKeeperMaster = getActiveKeeperMaster(clusterId, shardId);

		executeJob(new KeeperStateChangeJob(keepers, activeKeeperMaster, clientPool), new ActiveKeeperChanged(clusterId, shardId, oldActiveKeeper, activeKeeper));
		return true;
	}

	private InetSocketAddress getActiveKeeperMaster(String clusterId, String shardId) throws DaoException {
		
		RedisMeta redisMeta = metaServerDao.getRedisMaster(clusterId, shardId);
		if(redisMeta != null){
			return new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort());
		}
		String upstream = metaServerDao.getUpstream(clusterId, shardId);
		String []sp = upstream.split("\\s*:\\s*");
		if(sp.length != 2){
			throw  new IllegalStateException("upstream address error:" + clusterId + "," + shardId + "," + upstream);
		}
		return new InetSocketAddress(sp[0], Integer.parseInt(sp[1]));
	}


	@Override
	public boolean updateRedisMaster(String clusterId, String shardId, RedisMeta redisMaster) throws Exception {
		
		logger.info("[updateRedisMaster]{}, {}, {}", clusterId, shardId, redisMaster);
		if(!metaServerDao.updateRedisMaster(clusterId, shardId, redisMaster)){
			logger.info("[updateRedisMaster][redis master not changed]");
			return false;
		}

		KeeperMeta activeKeeper = metaServerDao.getKeeperActive(clusterId, shardId); 
		if(activeKeeper == null){
			throw new IllegalStateException("[promoteRedisMaster][active keeper not found!]" + clusterId + "," + shardId + "," + redisMaster);
		}
		
		RedisMeta oldRedisMaster = metaServerDao.getRedisMaster(clusterId, shardId);
		
		executeJob(new SlavePromotionJob(activeKeeper, redisMaster.getIp(), redisMaster.getPort(), clientPool), new RedisMasterChanged(clusterId, shardId, oldRedisMaster, redisMaster));
		
		return true;
	}

	private void executeJob(final Command<?> command, final Object event){
		
		executors.execute(new Runnable() {
			
			@Override
			public void run() {
				try {
					command.execute().sync();
					notifyObservers(event);
				} catch (Exception e) {
					logger.error("[run]" + command + "," + event, e);
				}
			}
		});
		
	}


	@Override
	public boolean updateUpstreamKeeper(String clusterId, String shardId, String address) throws Exception {
		
		logger.info("[updateUpstreamKeeper]{},{},{}", clusterId, shardId, address);
		
		if(!metaServerDao.updateUpstreamKeeper(clusterId, shardId, address)){
			logger.info("[updateUpstreamKeeper][not changed]{},{},{}", clusterId, shardId, address);
			return false;
		}
		
		KeeperMeta activeKeeper = metaServerDao.getKeeperActive(clusterId, shardId);
		
		SimpleObjectPool<NettyClient> pool =  new XpipeObjectPoolFromKeyed<InetSocketAddress, NettyClient>(clientPool, new InetSocketAddress(activeKeeper.getIp(), activeKeeper.getPort()));
		
		KeeperSetStateCommand command = new KeeperSetStateCommand(pool, KeeperState.ACTIVE, IpUtils.parseSingle(address));
		if(!command.execute().sync().get()){
			throw new RedisMetaServerException("updateUpstreamKeeper " + clusterId + " " + shardId + " "+ address + " fail, result not ok!");
		}
		return true;
	}

	@Override
	protected void doStop() throws Exception {

		for(MetaChangeListener metaChangeListener : metaChangeListeners){
			logger.info("[doStop][removeObserver]{}", metaChangeListener);
			removeObserver(metaChangeListener);
		}
	}
}
