package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerServiceFactory;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public class DefaultKeeperStateController extends AbstractLifecycle implements KeeperStateController, TopElement{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private int addKeeperSuccessTimeoutMilli =    180000;
	private int removeKeeperSuccessTimeoutMilli = 60000;
		
	@Autowired
	private KeeperContainerServiceFactory keeperContainerServiceFactory;
	
	@Autowired
	private DcMetaCache dcMetaCache;
	
	@Resource( name = MetaServerContextConfig.CLIENT_POOL )
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	
	@Resource( name = MetaServerContextConfig.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;
	
	private KeyedOneThreadTaskExecutor<Pair<String, String>> shardExecutor;
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		shardExecutor = new KeyedOneThreadTaskExecutor<>("DefaultKeeperStateController");
	}
	
	@Override
	public void addKeeper(KeeperTransMeta keeperTransMeta) {
		
		logger.info("[addKeeper]{}", keeperTransMeta);
		
		KeeperContainerService keeperContainerService = getKeeperContainerService(keeperTransMeta);
		shardExecutor.execute(new Pair<>(keeperTransMeta.getClusterId(), keeperTransMeta.getShardId()), 
				createAddKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, addKeeperSuccessTimeoutMilli));
	}

	protected Command<?> createAddKeeperCommand(KeeperContainerService keeperContainerService,
			KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled, int addKeeperSuccessTimeoutMilli) {
		return new AddKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, addKeeperSuccessTimeoutMilli);
	}

	@Override
	public void removeKeeper(KeeperTransMeta keeperTransMeta) {
		
		logger.info("[removeKeeper]{}", keeperTransMeta);
		KeeperContainerService keeperContainerService = getKeeperContainerService(keeperTransMeta);
		shardExecutor.execute(new Pair<>(keeperTransMeta.getClusterId(), keeperTransMeta.getShardId()),
				createDeleteKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, removeKeeperSuccessTimeoutMilli));
	}

	protected Command<?> createDeleteKeeperCommand(KeeperContainerService keeperContainerService,
			KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
			int removeKeeperSuccessTimeoutMilli) {
		return new DeleteKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, removeKeeperSuccessTimeoutMilli);
	}

	protected KeeperContainerService getKeeperContainerService(KeeperTransMeta keeperTransMeta) {
		
		KeeperContainerMeta keeperContainerMeta = dcMetaCache.getKeeperContainer(keeperTransMeta.getKeeperMeta());
		KeeperContainerService keeperContainerService = keeperContainerServiceFactory.getOrCreateKeeperContainerService(keeperContainerMeta);
		return keeperContainerService;
	}


	@Override
	protected void doDispose() throws Exception {
		shardExecutor.destroy();
		super.doDispose();
	}
}
