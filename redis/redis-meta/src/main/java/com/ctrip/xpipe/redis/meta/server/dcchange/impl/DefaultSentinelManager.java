package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
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
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
@Component
public class DefaultSentinelManager implements SentinelManager{
	
	private static int DEFAULT_SENTINEL_QUORUM = Integer.parseInt(System.getProperty("DEFAULT_SENTINEL_QUORUM", "3"));
	private static int DEFAULT_SENTINEL_ADD_SIZE = Integer.parseInt(System.getProperty("DEFAULT_SENTINEL_ADD_SIZE", "5"));

	public static int DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI = Integer.parseInt(System.getProperty("MIGRATE_SENTINEL_TIMEOUT", "100"));
	public static int DEFAULT_MIGRATION_SENTINEL_COMMAND_WAIT_TIMEOUT_MILLI = Integer.parseInt(System.getProperty("MIGRATE_SENTINEL_WAIT_TIMEOUT", "150"));
	private static Logger logger = LoggerFactory.getLogger(DefaultSentinelManager.class);
	
	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;
	
	@Autowired
	private DcMetaCache dcMetaCache;

	private ExecutorService executors = new DefaultExecutorFactory(getClass().getSimpleName(), OsUtils.getMultiCpuOrMax(2, 16), 2 * OsUtils.getCpuCount(),
			new ThreadPoolExecutor.AbortPolicy()).createExecutorService();

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

		ParallelCommandChain chain = new ParallelCommandChain(executors);
		for (int i = 0; i < addSize; i++) {
			chain.add(createSentinelAddCommand(sentinelMonitorName, redisMaster, quorum, new DefaultEndPoint(sentinels.get(i)), executionLog, clusterId, shardId));
		}
		try {
			chain.execute().get(DEFAULT_MIGRATION_SENTINEL_COMMAND_WAIT_TIMEOUT_MILLI, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			logger.warn("[addSentinels]{},{}", sentinelMonitorName, allSentinels, e);
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

		ParallelCommandChain chain = new ParallelCommandChain(executors);
		for (Sentinel sentinel : realSentinels) {
			chain.add(createSentinelRemoveCommand(sentinelMonitorName, new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()), executionLog));
		}
		try {
			chain.execute().get(DEFAULT_MIGRATION_SENTINEL_COMMAND_WAIT_TIMEOUT_MILLI, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			logger.warn("[removeSentinels]{}", realSentinels, e);
		}
	}

    SentinelRemove createSentinelRemoveCommand(String sentinelMonitorName, DefaultEndPoint sentinel, ExecutionLog executionLog) {
        SentinelRemove sentinelRemove = new SentinelRemove(keyedClientPool.getKeyPool(sentinel), sentinelMonitorName, scheduled, DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI);
        return (SentinelRemove) executionLog.trackCommand(this, sentinelRemove, String.format("removeSentinel %s from %s", sentinelMonitorName, sentinel));
    }

    SentinelAdd createSentinelAddCommand(String sentinelMonitorName, HostPort redisMaster, int quorum, DefaultEndPoint sentinel, ExecutionLog executionLog, String clusterId, String shardId) {
        SentinelAdd sentinelAdd = new SentinelAdd(keyedClientPool.getKeyPool(sentinel), sentinelMonitorName, redisMaster.getHost(), redisMaster.getPort(), quorum, scheduled, DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI);
        return (SentinelAdd) executionLog.trackCommand(this, sentinelAdd, String.format("add %s %s:%d %d to sentinel %s", sentinelMonitorName, redisMaster.getHost(), redisMaster.getPort(), quorum, sentinel));
    }

    private List<Sentinel> getRealSentinels(List<InetSocketAddress> sentinels, String sentinelMonitorName, ExecutionLog executionLog) {
		
		List<Sentinel> realSentinels = null;
		for(InetSocketAddress sentinelAddress: sentinels){
			
			SimpleObjectPool<NettyClient> clientPool = keyedClientPool.getKeyPool(new DefaultEndPoint(sentinelAddress));
			Sentinels sentinelsCommand = new Sentinels(clientPool, sentinelMonitorName, scheduled, DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI);
			try {
				realSentinels = sentinelsCommand.execute().get(DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI, TimeUnit.MILLISECONDS);
				executionLog.info(String.format("get sentinels from %s : %s", sentinelAddress, realSentinels));
				if(null != realSentinels) {
					realSentinels.add(new Sentinel(sentinelAddress.toString(), sentinelAddress.getHostString(), sentinelAddress.getPort()));
					break;
				}
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				logger.warn("[getRealSentinels]get sentinels from " + sentinelAddress, e);
				executionLog.warn("[getRealSentinels]get sentinels from " + sentinelAddress + "," + e.getMessage());
			}
		}
		
		
		return realSentinels;
	}

}
