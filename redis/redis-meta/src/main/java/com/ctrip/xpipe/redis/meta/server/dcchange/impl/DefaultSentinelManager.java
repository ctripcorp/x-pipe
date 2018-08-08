package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand.SentinelAdd;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand.SentinelRemove;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand.Sentinels;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.dcchange.exception.AddSentinelException;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
@Component
public class DefaultSentinelManager implements SentinelManager{
	
	private static int DEFAULT_SENTINEL_QUORUM = Integer.parseInt(System.getProperty("DEFAULT_SENTINEL_QUORUM", "3"));
	private static int DEFAULT_SENTINEL_ADD_SIZE = Integer.parseInt(System.getProperty("DEFAULT_SENTINEL_ADD_SIZE", "5"));

	private static Logger logger = LoggerFactory.getLogger(DefaultSentinelManager.class);
	
	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;
	
	@Autowired
	private DcMetaCache dcMetaCache;

	@Resource(name = MetaServerContextConfig.CLIENT_POOL)
	private XpipeNettyClientKeyedObjectPool keyedClientPool;
	
	public DefaultSentinelManager(){
		
	}
	
	public DefaultSentinelManager(DcMetaCache dcMetaCache, XpipeNettyClientKeyedObjectPool keyedClientPool) {
		this.dcMetaCache = dcMetaCache;
		this.keyedClientPool = keyedClientPool;
	}
	
	@Override
	public void addSentinel(String clusterId, String shardId, HostPort redisMaster, ExecutionLog executionLog) {
		
		String sentinelMonitorName = dcMetaCache.getSentinelMonitorName(clusterId, shardId);
		String allSentinels = dcMetaCache.getSentinel(clusterId, shardId).getAddress();
		
		executionLog.info(String.format("[addSentinel]%s,%s,%s, monitorName:%s, master:%s:%d",
				clusterId, shardId, allSentinels, sentinelMonitorName, redisMaster.getHost(), redisMaster.getPort()));
		
		if(checkEmpty(sentinelMonitorName, allSentinels, executionLog)){
			return;
		}
		
		int quorum = DEFAULT_SENTINEL_QUORUM;
		List<InetSocketAddress> sentinels = IpUtils.parse(allSentinels);
		
		if(sentinels.size() < quorum){
			throw new IllegalStateException(String.format("sentinel size < quorum, %d < %d", sentinels.size(), quorum));
		}
		
		int addSize = Math.min(sentinels.size(), DEFAULT_SENTINEL_ADD_SIZE);

		for(int i=0; i < addSize; i++){
			
			Endpoint sentinelAddress = new DefaultEndPoint(sentinels.get(i));
			SimpleObjectPool<NettyClient> clientPool = keyedClientPool.getKeyPool(sentinelAddress);
			SentinelAdd command = new SentinelAdd(clientPool, sentinelMonitorName, redisMaster.getHost(), redisMaster.getPort(), quorum, scheduled);
			try {
				String result = command.execute().get();
				executionLog.info(String.format("add to sentinel %s : %s", sentinelAddress, result));
			} catch (InterruptedException | ExecutionException e) {
				throw new AddSentinelException(sentinelAddress.getSocketAddress(), clusterId, shardId, redisMaster.getHost(), redisMaster.getPort(), e);
			}
		}
	}

	private boolean checkEmpty(String sentinelMonitorName, String allSentinels, ExecutionLog executionLog) {
		
		if(StringUtil.isEmpty(sentinelMonitorName)){
			executionLog.warn("sentinelMonitorName empty, exit!");
			return true;
		}
		
		if(StringUtil.isEmpty(allSentinels)){
			executionLog.warn("allSentinels empty, exit!");
			return true;
		}
		return false;
	}

	@Override
	public void removeSentinel(String clusterId, String shardId, ExecutionLog executionLog) {
		
		String sentinelMonitorName = dcMetaCache.getSentinelMonitorName(clusterId, shardId);
		String allSentinels = dcMetaCache.getSentinel(clusterId, shardId).getAddress();

		executionLog.info(String.format("removeSentinel cluster:%s, shard:%s, masterName:%s, sentinelAddress:%s", clusterId, shardId, sentinelMonitorName, allSentinels));

		if(checkEmpty(sentinelMonitorName, allSentinels, executionLog)){
			return;
		}

		List<InetSocketAddress> sentinels = IpUtils.parse(allSentinels);
		List<Sentinel> realSentinels = getRealSentinels(sentinels, sentinelMonitorName, executionLog);
		if(realSentinels == null){
			executionLog.warn("get real sentinels null");
			return;
		}
		
		executionLog.info(String.format("removeSentinel realSentinels:%s", realSentinels));
		
		for(Sentinel sentinel : realSentinels){
			
			SimpleObjectPool<NettyClient> clientPool = keyedClientPool.getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));
			SentinelRemove sentinelRemove = new SentinelRemove(clientPool, sentinelMonitorName, scheduled);
			try {
				String result = sentinelRemove.execute().get();
				executionLog.info(String.format("removeSentinel %s from %s : %s", sentinelMonitorName, sentinel, result));
			} catch (InterruptedException | ExecutionException e) {
				executionLog.info(String.format("removeSentinel %s from %s : %s", sentinelMonitorName, sentinel, e.getMessage()));
				logger.warn("[removeSentinel]" + sentinel, e);
			}
		}
	}

	private List<Sentinel> getRealSentinels(List<InetSocketAddress> sentinels, String sentinelMonitorName, ExecutionLog executionLog) {
		
		List<Sentinel> realSentinels = null;
		for(InetSocketAddress sentinelAddress: sentinels){
			
			SimpleObjectPool<NettyClient> clientPool = keyedClientPool.getKeyPool(new DefaultEndPoint(sentinelAddress));
			Sentinels sentinelsCommand = new Sentinels(clientPool, sentinelMonitorName, scheduled);
			try {
				realSentinels = sentinelsCommand.execute().get();
				executionLog.info(String.format("get sentinels from %s : %s", sentinelAddress, realSentinels));
				if(realSentinels.size() > 0){
					realSentinels.add(new Sentinel(sentinelAddress.toString(), sentinelAddress.getHostString(), sentinelAddress.getPort()));
					break;
				}
			} catch (InterruptedException | ExecutionException e) {
				logger.warn("[getRealSentinels]get sentinels from " + sentinelAddress, e);
				executionLog.warn("[getRealSentinels]get sentinels from " + sentinelAddress + "," + e.getMessage());
			}
		}
		
		
		return realSentinels;
	}

}
