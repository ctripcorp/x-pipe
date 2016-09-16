package com.ctrip.xpipe.redis.meta.server.keeper.manager;


import java.net.InetSocketAddress;
import java.util.LinkedList;
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
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerServiceFactory;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
@Component
public class DefaultKeeperStateController implements KeeperStateController{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
		
	@Autowired
	private KeeperContainerServiceFactory keeperContainerServiceFactory;
	
	@Autowired
	private DcMetaCache dcMetaCache;
	
	
	@Resource( name = "clientPool" )
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	
	private ExecutorService executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("KeeperStateController"));

	
	@Override
	public void addKeeper(KeeperTransMeta keeperTransMeta) {
		
		logger.info("[addKeeper]{}", keeperTransMeta);
		
		KeeperContainerService keeperContainerService = getKeeperContainerService(keeperTransMeta);
		keeperContainerService.addOrStartKeeper(keeperTransMeta);
	}

	@Override
	public void removeKeeper(KeeperTransMeta keeperTransMeta) {
		
		logger.info("[removeKeeper]{}", keeperTransMeta);

		KeeperContainerService keeperContainerService = getKeeperContainerService(keeperTransMeta);
		keeperContainerService.removeKeeper(keeperTransMeta);
	}

	private KeeperContainerService getKeeperContainerService(KeeperTransMeta keeperTransMeta) {
		
		KeeperContainerMeta keeperContainerMeta = dcMetaCache.getKeeperContainer(keeperTransMeta.getKeeperMeta());
		KeeperContainerService keeperContainerService = keeperContainerServiceFactory.getOrCreateKeeperContainerService(keeperContainerMeta);
		return keeperContainerService;
	}

	@Override
	public void makeKeeperActive(KeeperMeta keeperMeta, InetSocketAddress redisMasterAddress) {

		logger.info("[makeKeeperActive]{}, {}", keeperMeta, redisMasterAddress);

		KeeperMeta targetMeta = MetaClone.clone(keeperMeta);
		List<KeeperMeta> keepers = new LinkedList<>();
		targetMeta.setActive(true);
		keepers.add(targetMeta);
		
		executeJob(new KeeperStateChangeJob(keepers, redisMasterAddress, clientPool));
	}

	@Override
	public void makeSureKeeperStateRight(List<KeeperMeta> targetKeeperMetas, InetSocketAddress redisMasterAddress) {

		logger.info("[makeSureKeeperStateRight]{},{}", targetKeeperMetas, redisMasterAddress);
		
		executeJob(new KeeperStateChangeJob(targetKeeperMetas, redisMasterAddress, clientPool));
	}

	private void executeJob(final Command<?> command){
		
		executors.execute(new Runnable() {
			
			@Override
			public void run() {
				try {
					command.execute().sync();
				} catch (Exception e) {
					logger.error("[run]" + command , e);
				}
			}
		});
	}

	@Override
	public void makeKeeperBackup(KeeperMeta keeperMeta, KeeperMeta activeKeeper) {
		
		KeeperMeta targetMeta = MetaClone.clone(keeperMeta);
		KeeperMeta activeMeta = MetaClone.clone(activeKeeper);
		
		List<KeeperMeta> keepers = new LinkedList<>();
		targetMeta.setActive(false);
		activeMeta.setActive(true);
		keepers.add(targetMeta);
		keepers.add(activeMeta);
		
		executeJob(new KeeperStateChangeJob(keepers, null, clientPool));
	}

}
