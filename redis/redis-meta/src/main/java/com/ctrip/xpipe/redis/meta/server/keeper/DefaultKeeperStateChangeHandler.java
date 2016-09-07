package com.ctrip.xpipe.redis.meta.server.keeper;

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
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.impl.MetaChangeListener;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Jul 8, 2016
 */
@Component
public class DefaultKeeperStateChangeHandler extends AbstractLifecycleObservable implements MetaServerStateChangeHandler{
	
	protected static Logger logger = LoggerFactory.getLogger(DefaultKeeperStateChangeHandler.class);
	
	@Resource( name = "clientPool" )
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	
	private ExecutorService executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("META-SERVICE"));
	
	@Autowired
	private List<MetaChangeListener>  metaChangeListeners;

	@Autowired
	private CurrentMetaManager  currentMetaServerMetaManager;
	
	
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
		
		List<KeeperMeta> keepers = currentMetaServerMetaManager.getSurviveKeepers(clusterId, shardId);
		if(keepers == null || keepers.size() == 0){
			logger.info("[keeperActiveElected][none keeper survice, do nothing]");
			return;
		}
		InetSocketAddress activeKeeperMaster = getActiveKeeperMaster(clusterId, shardId);

		executeJob(new KeeperStateChangeJob(keepers, activeKeeperMaster, clientPool));
	}

	private InetSocketAddress getActiveKeeperMaster(String clusterId, String shardId) throws MetaException {
		

		RedisMeta redisMeta = currentMetaServerMetaManager.getRedisMaster(clusterId, shardId);
		if(redisMeta != null){
			return new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort());
		}
		String upstream = currentMetaServerMetaManager.getUpstream(clusterId, shardId);
		String []sp = upstream.split("\\s*:\\s*");
		if(sp.length != 2){
			throw  new IllegalStateException("upstream address error:" + clusterId + "," + shardId + "," + upstream);
		}
		return new InetSocketAddress(sp[0], Integer.parseInt(sp[1]));
	}


	private void executeJob(final Command<?> command){
		
		executors.execute(new Runnable() {
			
			@Override
			public void run() {
				try {
					logger.info("[run]" + command);
					command.execute().sync();
				} catch (Exception e) {
					logger.error("[run]" + command, e);
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
