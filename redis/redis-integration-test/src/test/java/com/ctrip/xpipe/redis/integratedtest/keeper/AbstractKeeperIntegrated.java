package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.*;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperGetStateCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperSetStateCommand;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTest;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;

import java.util.concurrent.ExecutionException;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public abstract class AbstractKeeperIntegrated extends AbstractIntegratedTest{

	protected int replicationStoreCommandFileSize = 1024;
	private int replicationStoreCommandFileNumToKeep = 2;
	private int replicationStoreMaxCommandsToTransferBeforeCreateRdb = 1024;
	private int minTimeMilliToGcAfterCreate = 2000;

	protected KeeperMeta getKeeperActive(RedisMeta redisMeta) {
		
		for(KeeperMeta keeper : redisMeta.parent().getKeepers()){
			if(keeper.isActive()){
				return keeper;
			}
		}
		return null;
	}

	protected void setKeeperState(KeeperMeta keeperMeta, KeeperState keeperState, String ip, Integer port) throws Exception {
		setKeeperState(keeperMeta, keeperState, ip, port, true);
	}

	protected void setKeeperState(KeeperMeta keeperMeta, KeeperState keeperState, String ip, Integer port, boolean sync) throws Exception {
		KeeperSetStateCommand command = new KeeperSetStateCommand(keeperMeta, keeperState, new Pair<String, Integer>(ip, port), scheduled);
		CommandFuture<?> future = command.execute();
		if(sync){
			future.sync();
		}
	}

	protected KeeperState getKeeperState(KeeperMeta keeperMeta) throws Exception {
		KeeperGetStateCommand command = new KeeperGetStateCommand(keeperMeta, scheduled);
		return command.execute().get();
	}

	
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(replicationStoreCommandFileSize, replicationStoreCommandFileNumToKeep, 
				replicationStoreMaxCommandsToTransferBeforeCreateRdb, minTimeMilliToGcAfterCreate);
	}

	protected void setRedisToGtidEnabled(String ip, Integer port) throws Exception {
		setRedisToGtidEnabled(ip, port, true);
	}

	protected void setRedisToGtidNotEnabled(String ip, Integer port) throws Exception {
		setRedisToGtidEnabled(ip, port, false);
	}

	protected void setRedisToGtidEnabled(String ip, Integer port, boolean enabled) throws Exception {
		SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
		ConfigSetCommand.ConfigSetGtidEnabled configSetGtidEnabled = new ConfigSetCommand.ConfigSetGtidEnabled(enabled, keyPool, scheduled);
		String gtid = configSetGtidEnabled.execute().get().toString();
		logger.info("[setRedisToGtidEnabled] {}", gtid);
	}

	protected boolean isRedisGtidEnabled(String ip, Integer port) throws Exception {
		SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
		ConfigGetCommand.ConfigGetGtidEnabled configGetGtidEnabled = new ConfigGetCommand.ConfigGetGtidEnabled(keyPool, scheduled);
		return configGetGtidEnabled.execute().get();
	}

	protected String getGtidSet(String ip, int port, String key) throws ExecutionException, InterruptedException {
		SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(ip, port));
		InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.GTID, scheduled);
		String value = infoCommand.execute().get();
		logger.info("get gtid set from {}, {}, {}", ip, port, value);
		String gtidSet = new InfoResultExtractor(value).extract(key);
		return gtidSet;
	}

	protected Long getOffset(String ip, int port, boolean master) throws ExecutionException, InterruptedException {
		SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(ip, port));
		InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled);
		String value = infoCommand.execute().get();
		logger.info("get gtid set from {}, {}, {}", ip, port, value);
		String gtidSet;
		if(master) {
			gtidSet = new InfoResultExtractor(value).extract("master_repl_offset");
		} else {
			gtidSet = new InfoResultExtractor(value).extract("slave_repl_offset");
		}
		return Long.parseLong(gtidSet);
	}

	protected void assertReplOffset(RedisMeta master) throws Exception {
		long masterOffset = getOffset(master.getIp(), master.getPort(), true);
		for(RedisMeta slave: getRedisSlaves()) {
			long slaveOffset = getOffset(slave.getIp(), slave.getPort(), false);
			logger.info("slave {}:{} gtid set: {}", slave.getIp(), slave.getPort(), slaveOffset);
			Assert.assertEquals(masterOffset, slaveOffset);
		}
	}

	protected void setRedisMaster(RedisMeta redis, HostPort redisMaster) throws Exception {
		SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(redis.getIp(), redis.getPort()));
		new SlaveOfCommand(slaveClientPool, redisMaster.getHost(), redisMaster.getPort(), scheduled).execute().get();
	}

}
