package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.KeeperMasterCheckNotAsExpectedException;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;

/**
 * @author wenchao.meng
 *
 * Jul 8, 2016
 */
public class KeeperMasterCheckJob extends AbstractCommand<Void> implements RequestResponseCommand<Void> {

	private Long clusterId;
	private Long shardId;
	private DcMetaCache dcMetaCache;
	private Pair<String, Integer> activeKeeperMaster;
	private int retryDelayMilli;
	private int retryTimes;
	private RetryCommandFactory retryCommandFactory;
	private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;
	private ScheduledExecutorService scheduled;

	public KeeperMasterCheckJob(Long clusterId,
								Long shardId,
								DcMetaCache dcMetaCache,
								Pair<String, Integer> activeKeeperMaster,
                                SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool
			, ScheduledExecutorService scheduled){
		this(clusterId, shardId, dcMetaCache, activeKeeperMaster, clientPool, 10, 1, scheduled);
	}

	public KeeperMasterCheckJob(Long clusterId,
								Long shardId,
								DcMetaCache dcMetaCache,
								Pair<String, Integer> activeKeeperMaster,
                                SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool
			, int retryDelayMilli, int retryTimes, ScheduledExecutorService scheduled){
		this.clusterId = clusterId;
		this.shardId = shardId;
		this.dcMetaCache = dcMetaCache;
		this.activeKeeperMaster = activeKeeperMaster;
		this.clientPool = clientPool;
		this.retryDelayMilli = retryDelayMilli;
		this.retryTimes = retryTimes;
		this.scheduled = scheduled;
		this.retryCommandFactory = DefaultRetryCommandFactory.retryNTimes(scheduled, retryTimes, retryDelayMilli);
	}

	@Override
	public String getName() {
		return "keeper master check job";
	}

	@Override
	@SuppressWarnings({ "unchecked"})
	protected void doExecute() throws CommandExecutionException {
		if (!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)) {
			future().setSuccess(null);
			return;
		}
		if (!isRedis(clusterId, shardId, activeKeeperMaster.getKey(), activeKeeperMaster.getValue())) {
			future().setFailure(new KeeperMasterCheckNotAsExpectedException(activeKeeperMaster.getKey(), activeKeeperMaster.getValue(), "not redis"));
			return;
		}
		Command<Role> roleCommand = retryCommandFactory.createRetryCommand(new RoleCommand(clientPool.getKeyPool(new DefaultEndPoint(activeKeeperMaster.getKey(), activeKeeperMaster.getValue())), DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI, false, scheduled));
		roleCommand.execute().addListener(commandFuture -> {
			if (commandFuture.isSuccess()) {
				if (commandFuture.get().getServerRole() == Server.SERVER_ROLE.MASTER) {
					future().setSuccess(null);
				} else {
					future().setFailure(new KeeperMasterCheckNotAsExpectedException(activeKeeperMaster.getKey(), activeKeeperMaster.getValue(), "not master"));
				}
			} else {
				future().setFailure(new KeeperMasterCheckNotAsExpectedException(activeKeeperMaster.getKey(), activeKeeperMaster.getValue(), commandFuture.cause()));
			}
		});
	}

	protected boolean isRedis(Long clusterDbId, Long shardDbId, String ip, int port) {
		if (clusterDbId == null || shardDbId == null || ip == null || ip.isEmpty()) {
			return false;
		}
		List<RedisMeta> shardRedis = dcMetaCache.getShardRedises(clusterDbId, shardDbId);
		for (RedisMeta redis : shardRedis) {
			if (redis.getIp().equals(ip) && redis.getPort() == port) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected void doReset(){
	}

	@Override
	public int getCommandTimeoutMilli() {
		return DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI * (1 + retryTimes) + retryDelayMilli * retryTimes;
	}
}
