package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.KeeperMasterCheckNotAsExpectedException;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;

public class KeeperMasterProcessJob extends AbstractCommand<Void> implements RequestResponseCommand<Void> {

	private Long clusterDbId;
	private Long shardDbId;
	private DcMetaCache dcMetaCache;

	private Pair<String, Integer> activeKeeperMaster;
	private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;
	private ScheduledExecutorService scheduled;
	private Executor executors;
	private List<KeeperMeta> keepers;
	private RouteMeta routeForActiveKeeper;


	public KeeperMasterProcessJob(Long clusterId,
                                  Long shardId,
								  List<KeeperMeta> keepers,
								  RouteMeta routeForActiveKeeper,
                                  DcMetaCache dcMetaCache,
                                  Pair<String, Integer> activeKeeperMaster,
                                  SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool,
								  ScheduledExecutorService scheduled, Executor executors){
		this.clusterDbId = clusterId;
		this.shardDbId = shardId;
		this.keepers = keepers;
		this.routeForActiveKeeper = routeForActiveKeeper;
		this.dcMetaCache = dcMetaCache;
		this.activeKeeperMaster = activeKeeperMaster;
		this.clientPool = clientPool;
		this.scheduled = scheduled;
		this.executors = executors;
	}

	@Override
	public String getName() {
		return "KeeperMasterProcessJob";
	}

	@Override
	protected void doExecute() throws CommandExecutionException {
		SequenceCommandChain chain = new SequenceCommandChain(false);
		KeeperMasterCheckJob checkJob = new KeeperMasterCheckJob(clusterDbId, shardDbId, dcMetaCache, activeKeeperMaster, clientPool, executors, scheduled);
		KeeperStateChangeJob changeJob = new KeeperStateChangeJob(keepers, activeKeeperMaster, routeForActiveKeeper, clientPool, scheduled, executors);
		chain.add(checkJob);
		chain.add(changeJob);
		chain.execute().addListener(commandFuture -> {
            if(commandFuture.isSuccess()){
                future().setSuccess(null);
            } else {
                future().setFailure(commandFuture.cause().getCause());
				getLogger().info("[KeeperMasterProcessJob][fail] clusterId:{}, shardId:{}, error:{}", clusterDbId, shardDbId, commandFuture.cause());

				String alertMessage = Optional.ofNullable(commandFuture.cause().getMessage())
						.filter(msg -> msg.lastIndexOf("error:") != -1)
						.map(msg -> msg.substring(msg.lastIndexOf("error:") + 6))
						.orElse("exception occurred");
				EventMonitor.DEFAULT.logAlertEvent("keeper.master.process:" + alertMessage);
            }
        });
	}

	@Override
	protected void doReset(){
	}

	@Override
	public int getCommandTimeoutMilli() {
		return DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI * 3;
	}
}
