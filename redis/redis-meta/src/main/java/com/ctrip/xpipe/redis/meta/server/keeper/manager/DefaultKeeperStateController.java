package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import java.net.InetSocketAddress;
import java.util.Map;

import javax.annotation.Resource;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.concurrent.OneThreadTaskExecutor;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerServiceFactory;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.utils.MapUtils;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
@Component
public class DefaultKeeperStateController implements KeeperStateController{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private int addKeeperSuccessTimeoutMilli =    180000;
	private int removeKeeperSuccessTimeoutMilli = 60000;
		
	@Autowired
	private KeeperContainerServiceFactory keeperContainerServiceFactory;
	
	@Autowired
	private CurrentMetaManager currentMetaManager;
	
	@Autowired
	private DcMetaCache dcMetaCache;
	
	@Resource( name = "clientPool" )
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	
	private Map<Pair<String, String>, OneThreadTaskExecutor> shardExecutor = new ConcurrentHashMap<>();
	
	@Override
	public void addKeeper(KeeperTransMeta keeperTransMeta) {
		
		logger.info("[addKeeper]{}", keeperTransMeta);
		
		KeeperContainerService keeperContainerService = getKeeperContainerService(keeperTransMeta);
		OneThreadTaskExecutor oneThreadTaskExecutor = getOrCreate(keeperTransMeta.getClusterId(), keeperTransMeta.getShardId());
		oneThreadTaskExecutor.executeCommand(new AddKeeperCommand(keeperContainerService, keeperTransMeta, addKeeperSuccessTimeoutMilli));
	}

	private OneThreadTaskExecutor getOrCreate(String clusterId, String shardId) {
		return MapUtils.getOrCreate(shardExecutor, new Pair<>(clusterId, shardId), new ObjectFactory<OneThreadTaskExecutor>() {

			@Override
			public OneThreadTaskExecutor create() {
				return new OneThreadTaskExecutor("[DefaultKeeperStateController]");
			}
		});
	}

	@Override
	public void removeKeeper(KeeperTransMeta keeperTransMeta) {
		
		logger.info("[removeKeeper]{}", keeperTransMeta);
		
		KeeperContainerService keeperContainerService = getKeeperContainerService(keeperTransMeta);
		OneThreadTaskExecutor oneThreadTaskExecutor = getOrCreate(keeperTransMeta.getClusterId(), keeperTransMeta.getShardId());
		oneThreadTaskExecutor.executeCommand(new DeleteKeeperCommand(currentMetaManager, keeperContainerService, keeperTransMeta, removeKeeperSuccessTimeoutMilli));
	}

	private KeeperContainerService getKeeperContainerService(KeeperTransMeta keeperTransMeta) {
		
		KeeperContainerMeta keeperContainerMeta = dcMetaCache.getKeeperContainer(keeperTransMeta.getKeeperMeta());
		KeeperContainerService keeperContainerService = keeperContainerServiceFactory.getOrCreateKeeperContainerService(keeperContainerMeta);
		return keeperContainerService;
	}
}
