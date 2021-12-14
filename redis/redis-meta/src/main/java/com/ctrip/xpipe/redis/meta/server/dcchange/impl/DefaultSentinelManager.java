package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
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
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
@Component
public class DefaultSentinelManager implements SentinelManager{
	
	private static int DEFAULT_SENTINEL_QUORUM = Integer.parseInt(System.getProperty("DEFAULT_SENTINEL_QUORUM", "3"));
	private static int DEFAULT_SENTINEL_ADD_SIZE = Integer.parseInt(System.getProperty("DEFAULT_SENTINEL_ADD_SIZE", "5"));

	public static int DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI = Integer.parseInt(System.getProperty("MIGRATE_SENTINEL_TIMEOUT", "500"));
	public static int DEFAULT_MIGRATION_SENTINEL_COMMAND_WAIT_TIMEOUT_MILLI = Integer.parseInt(System.getProperty("MIGRATE_SENTINEL_WAIT_TIMEOUT", "550"));
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
	public void addSentinel(Long clusterDbId, Long shardDbId, HostPort redisMaster, ExecutionLog executionLog) {
		
		String sentinelMonitorName = dcMetaCache.getSentinelMonitorName(clusterDbId, shardDbId);
		String allSentinels = dcMetaCache.getSentinel(clusterDbId, shardDbId).getAddress();
		
		executionLog.info(String.format("[addSentinel]%s,%s,%s, monitorName:%s, master:%s:%d",
				clusterDbId, shardDbId, allSentinels, sentinelMonitorName, redisMaster.getHost(), redisMaster.getPort()));
		
		if(checkEmpty(sentinelMonitorName, allSentinels, executionLog)){
			return;
		}
		
		int quorum = DEFAULT_SENTINEL_QUORUM;
		List<InetSocketAddress> sentinels = IpUtils.parse(allSentinels);
		
		if(sentinels.size() < quorum){
			throw new IllegalStateException(String.format("sentinel size < quorum, %d < %d", sentinels.size(), quorum));
		}
		
		int addSize = Math.min(sentinels.size(), DEFAULT_SENTINEL_ADD_SIZE);

		ParallelCommandChain chain = new ParallelCommandChain(MoreExecutors.directExecutor());
		for (int i = 0; i < addSize; i++) {
			chain.add(createSentinelAddCommand(sentinelMonitorName, redisMaster, quorum, new DefaultEndPoint(sentinels.get(i)), executionLog));
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
	public void removeSentinel(Long clusterDbId, Long shardDbId, ExecutionLog executionLog) {
		
		String sentinelMonitorName = dcMetaCache.getSentinelMonitorName(clusterDbId, shardDbId);
		String allSentinels = dcMetaCache.getSentinel(clusterDbId, shardDbId).getAddress();

		executionLog.info(String.format("removeSentinel cluster:%s, shard:%s, masterName:%s, sentinelAddress:%s", clusterDbId, shardDbId, sentinelMonitorName, allSentinels));

		if(checkEmpty(sentinelMonitorName, allSentinels, executionLog)){
			return;
		}

		List<InetSocketAddress> sentinels = IpUtils.parse(allSentinels);
		List<Sentinel> realSentinels = getRealSentinels(sentinels, sentinelMonitorName, executionLog);
		
		executionLog.info(String.format("removeSentinel sentinels:%s", realSentinels));

		ParallelCommandChain chain = new ParallelCommandChain(MoreExecutors.directExecutor());
		for (Sentinel sentinel : realSentinels) {
			chain.add(createSentinelRemoveCommand(sentinelMonitorName, new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()), executionLog));
		}
		try {
			chain.execute().get(DEFAULT_MIGRATION_SENTINEL_COMMAND_WAIT_TIMEOUT_MILLI, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			logger.warn("[removeSentinels]{},{}", sentinelMonitorName, realSentinels, e);
		}
	}

    SentinelRemove createSentinelRemoveCommand(String sentinelMonitorName, DefaultEndPoint sentinel, ExecutionLog executionLog) {
        SentinelRemove sentinelRemove = new SentinelRemove(keyedClientPool.getKeyPool(sentinel), sentinelMonitorName, scheduled, DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI);
        return (SentinelRemove) executionLog.trackCommand(this, sentinelRemove, String.format("removeSentinel %s from %s", sentinelMonitorName, sentinel));
    }

    SentinelAdd createSentinelAddCommand(String sentinelMonitorName, HostPort redisMaster, int quorum, DefaultEndPoint sentinel, ExecutionLog executionLog) {
        SentinelAdd sentinelAdd = new SentinelAdd(keyedClientPool.getKeyPool(sentinel), sentinelMonitorName, redisMaster.getHost(), redisMaster.getPort(), quorum, scheduled, DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI);
        return (SentinelAdd) executionLog.trackCommand(this, sentinelAdd, String.format("add %s %s:%d %d to sentinel %s", sentinelMonitorName, redisMaster.getHost(), redisMaster.getPort(), quorum, sentinel));
    }


	private List<Sentinel> getRealSentinels(List<InetSocketAddress> sentinels, String sentinelMonitorName, ExecutionLog executionLog) {

		Set<Sentinel> realSentinels = Collections.synchronizedSet(new HashSet<>());
		ParallelCommandChain chain = new ParallelCommandChain(MoreExecutors.directExecutor());
		for (InetSocketAddress sentinelAddress : sentinels) {
			chain.add(createSentinelsCommand(new DefaultEndPoint(sentinelAddress), sentinelMonitorName, realSentinels, executionLog));
		}
		try {
			chain.execute().get(DEFAULT_MIGRATION_SENTINEL_COMMAND_WAIT_TIMEOUT_MILLI, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			logger.warn("[getRealSentinels]{},{}", sentinels, sentinelMonitorName, e);
		}
		if (realSentinels.isEmpty()) {
			executionLog.warn("get real sentinels null, use config sentinels");
			for (InetSocketAddress sentinelAddress : sentinels) {
				realSentinels.add(new Sentinel(sentinelAddress.toString(), sentinelAddress.getHostString(), sentinelAddress.getPort()));
			}
		}

		return Lists.newArrayList(realSentinels);
	}

    Sentinels createSentinelsCommand(DefaultEndPoint sentinel, String sentinelMonitorName, final Set<Sentinel> realSentinels, ExecutionLog executionLog) {
        Sentinels sentinelsCommand = new Sentinels(keyedClientPool.getKeyPool(sentinel), sentinelMonitorName, scheduled, DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI);
        executionLog.trackCommand(this, sentinelsCommand, String.format("get sentinels from %s", sentinel));
        sentinelsCommand.future().addListener(commandFuture -> {
            if (commandFuture.isSuccess())
                realSentinels.addAll(commandFuture.get());
        });
        return sentinelsCommand;
    }

}
