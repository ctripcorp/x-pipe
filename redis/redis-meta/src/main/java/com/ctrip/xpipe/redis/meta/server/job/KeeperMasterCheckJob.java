package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.CommandFuture;
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
import java.util.Objects;
import java.util.concurrent.*;


/**
 * @author wenchao.meng
 *
 * Jul 8, 2016
 */
public class KeeperMasterCheckJob extends AbstractCommand<Void> {

	private Long clusterId;
	private Long shardId;
	private DcMetaCache dcMetaCache;
	private Pair<String, Integer> activeKeeperMaster;
	private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;
	private ScheduledExecutorService scheduled;
	private Executor executors;
	private int checkRedisTimeoutSeconds = 1;

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
	@SuppressWarnings("rawtypes")
    protected void doExecute() throws CommandExecutionException {
		if (!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)) {
			future().setSuccess(null);
			return;
		}
		if (!isRedis(clusterId, shardId, activeKeeperMaster.getKey(), activeKeeperMaster.getValue())) {
			setFutureFailure(activeKeeperMaster, "not redis");
			return;
		}
		RoleCommand masterCommand = new RoleCommand(clientPool.getKeyPool(new DefaultEndPoint(activeKeeperMaster.getKey(), activeKeeperMaster.getValue())), checkRedisTimeoutSeconds * 1000, false, scheduled);
		masterCommand.execute().addListener(masterFuture -> {
			if (masterFuture.isSuccess()) {
				Role masterRole = masterFuture.getNow();
				if (masterRole == null || masterRole.getServerRole() != Server.SERVER_ROLE.MASTER) {
					setFutureFailure(activeKeeperMaster, "not master");
				} else {
					List<RedisMeta> redises = dcMetaCache.getShardRedises(clusterId, shardId);

					ParallelCommandChain roleChain = new ParallelCommandChain(executors);

					for (RedisMeta redisMeta : redises) {
						if (redisMeta.getIp().equals(activeKeeperMaster.getKey()) && Objects.equals(redisMeta.getPort(), activeKeeperMaster.getValue())) {
							continue;
						}
						RoleCommand command = new RoleCommand(clientPool.getKeyPool(new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort())), checkRedisTimeoutSeconds * 1000, false, scheduled);
						roleChain.add(command);
					}

					roleChain.execute().addListener(redisFuture -> {
						boolean hasMaster = false;
						for (CommandFuture future : roleChain.getResult()) {
							Role role = (Role) future.getNow();
							if (role != null && role.getServerRole() == Server.SERVER_ROLE.MASTER) {
								hasMaster = true;
								break;
							}
						}
						if (hasMaster) {
							setFutureFailure(activeKeeperMaster, "two master");
						} else {
							future().setSuccess(null);
						}
					});
				}
			} else {
				setFutureFailure(activeKeeperMaster, "check master fail");
			}
		});
	}

	private void setFutureFailure(Pair<String, Integer> activeKeeperMaster, String message) {
		future().setFailure(new KeeperMasterCheckNotAsExpectedException(activeKeeperMaster.getKey(), activeKeeperMaster.getValue(), message));
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
