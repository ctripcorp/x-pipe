package com.ctrip.xpipe.redis.meta.server.job;

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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;
import static com.ctrip.xpipe.redis.meta.server.job.KEEPER_ALERT.*;

public class KeeperMasterCheckJob extends AbstractCommand<Void> {

	private Long clusterId;
	private Long shardId;
	private DcMetaCache dcMetaCache;
	private Pair<String, Integer> activeKeeperMaster;
	private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;
	private ScheduledExecutorService scheduled;
	private Executor executors;

	public KeeperMasterCheckJob(Long clusterId,
								Long shardId,
								DcMetaCache dcMetaCache,
								Pair<String, Integer> activeKeeperMaster,
                                SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool,
								Executor executors
			, ScheduledExecutorService scheduled){
		this.clusterId = clusterId;
		this.shardId = shardId;
		this.dcMetaCache = dcMetaCache;
		this.activeKeeperMaster = activeKeeperMaster;
		this.clientPool = clientPool;
		this.scheduled = scheduled;
		this.executors = executors;
	}

	@Override
	public String getName() {
		return "KeeperMasterCheckJob";
	}

	@Override
    protected void doExecute() throws CommandExecutionException {
		if (!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)) {
			future().setSuccess(null);
			return;
		}
		if (!isRedis(clusterId, shardId, activeKeeperMaster.getKey(), activeKeeperMaster.getValue())) {
			setFutureFailure(activeKeeperMaster, CHECK_NOT_REDIS);
			return;
		}

		Map<Pair<String, Integer>, Role> roleResult = new ConcurrentHashMap<>();
		List<RedisMeta> shardRedis = dcMetaCache.getShardRedises(clusterId, shardId);
		ParallelCommandChain chain = new ParallelCommandChain(executors);
		for (RedisMeta redisMeta : shardRedis) {
			RoleCommand command = new RoleCommand(clientPool.getKeyPool(new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort())), DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI, false, scheduled);
			command.future().addListener(commandFuture -> {
				if (commandFuture.isSuccess()) {
					roleResult.put(Pair.of(redisMeta.getIp(), redisMeta.getPort()), commandFuture.getNow());
				}
			});
			chain.add(command);
		}
		chain.execute().addListener(redisFuture -> {
			List<RedisMeta> masterList = shardRedis.stream().filter(redisMeta -> roleResult.containsKey(Pair.of(redisMeta.getIp(), redisMeta.getPort())) &&
					roleResult.get(Pair.of(redisMeta.getIp(), redisMeta.getPort())).getServerRole() == Server.SERVER_ROLE.MASTER).collect(Collectors.toList());
			if (masterList.size() > 1) {
				setFutureFailure(activeKeeperMaster, CHECK_MULTI_MASTER);
			} else {
				if (masterList.size() == 1 && masterList.get(0).getIp().equals(activeKeeperMaster.getKey()) && Objects.equals(masterList.get(0).getPort(), activeKeeperMaster.getValue())) {
					future().setSuccess(null);
				} else {
					setFutureFailure(activeKeeperMaster, CHECK_NOT_MASTER);
				}
			}
		});
	}

	private void setFutureFailure(Pair<String, Integer> activeKeeperMaster, KEEPER_ALERT alert) {
		future().setFailure(new KeeperMasterCheckNotAsExpectedException(activeKeeperMaster.getKey(), activeKeeperMaster.getValue(), alert));
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
}
