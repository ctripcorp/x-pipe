package com.ctrip.xpipe.redis.meta.server.impl;

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
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaException;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;
import com.ctrip.xpipe.redis.meta.server.MetaServerEventsHandler;
import com.ctrip.xpipe.redis.meta.server.impl.event.ActiveKeeperChanged;
import com.ctrip.xpipe.redis.meta.server.impl.event.RedisMasterChanged;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaServerMetaManager;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Jul 8, 2016
 */
@Component
public class DefaultMetaServerEventsHandler extends AbstractLifecycleObservable implements MetaServerEventsHandler{
	
	protected static Logger logger = LoggerFactory.getLogger(DefaultMetaServerEventsHandler.class);
	
	@Resource( name = "clientPool" )
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	
	private ExecutorService executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("META-SERVICE"));
	
	@Autowired
	private List<MetaChangeListener>  metaChangeListeners;

	@Autowired
	private CurrentMetaServerMetaManager  currentMetaServerMetaManager;
	
	
	@Override
	protected void doStart() throws Exception {
		
		for(MetaChangeListener metaChangeListener : metaChangeListeners){
			logger.info("[doStart][addObserver]{}", metaChangeListener);
			addObserver(metaChangeListener);
		}
	}
	
	@Override
	public void keeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper) throws Exception {
		
		logger.info("[keeperActiveElected]{},{},{}", clusterId, shardId, activeKeeper);
		DcMetaManager currentMeta = currentMetaServerMetaManager.getCurrentMeta();
		
		KeeperMeta oldActiveKeeper = currentMeta.getKeeperActive(clusterId, shardId);
		
		if(!currentMeta.updateKeeperActive(clusterId, shardId, activeKeeper)){
			logger.info("[updateKeeperActive][keeper active not changed]");
			return;
		}
		//make sure keeper in proper state
		List<KeeperMeta> keepers = currentMeta.getKeepers(clusterId, shardId);
		InetSocketAddress activeKeeperMaster = getActiveKeeperMaster(clusterId, shardId);

		executeJob(new KeeperStateChangeJob(keepers, activeKeeperMaster, clientPool), new ActiveKeeperChanged(clusterId, shardId, oldActiveKeeper, activeKeeper));
	}

	private InetSocketAddress getActiveKeeperMaster(String clusterId, String shardId) throws MetaException {
		
		DcMetaManager currentMeta = currentMetaServerMetaManager.getCurrentMeta();

		RedisMeta redisMeta = currentMeta.getRedisMaster(clusterId, shardId);
		if(redisMeta != null){
			return new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort());
		}
		String upstream = currentMeta.getUpstream(clusterId, shardId);
		String []sp = upstream.split("\\s*:\\s*");
		if(sp.length != 2){
			throw  new IllegalStateException("upstream address error:" + clusterId + "," + shardId + "," + upstream);
		}
		return new InetSocketAddress(sp[0], Integer.parseInt(sp[1]));
	}

	@Override
	public void redisMasterChanged(String clusterId, String shardId, RedisMeta redisMaster) throws Exception {
		
		DcMetaManager currentMeta = currentMetaServerMetaManager.getCurrentMeta();

		logger.info("[redisMasterChanged]{}, {}, {}", clusterId, shardId, redisMaster);
		
		RedisMeta oldRedisMaster = currentMeta.getRedisMaster(clusterId, shardId);
		if(!currentMeta.updateRedisMaster(clusterId, shardId, redisMaster)){
			logger.info("[redisMasterChanged][redis master not changed]");
			return ;
		}
		
		notifyObservers(new RedisMasterChanged(clusterId, shardId, oldRedisMaster, redisMaster));
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
	protected void doStop() throws Exception {

		for(MetaChangeListener metaChangeListener : metaChangeListeners){
			logger.info("[doStop][removeObserver]{}", metaChangeListener);
			removeObserver(metaChangeListener);
		}
	}
}
