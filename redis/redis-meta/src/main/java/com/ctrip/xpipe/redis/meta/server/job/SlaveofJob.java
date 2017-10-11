package com.ctrip.xpipe.redis.meta.server.job;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Oct 28, 2016
 */
public class SlaveofJob extends AbstractRedisesSlaveofJob{

	public SlaveofJob(List<RedisMeta> slaves, String masterHost, int masterPort,
					  SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool, ScheduledExecutorService scheduled, Executor executors) {
		super(slaves, masterHost, masterPort, clientPool, scheduled, executors);
	}

	@Override
	protected Command<?> createSlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, String masterHost, int masterPort) {
		return new SlaveOfCommand(clientPool, masterHost, masterPort, scheduled);
	}

}
