package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 *         May 9, 2016 3:19:37 PM
 */
public class SlaveOfCommand extends AbstractSlaveOfCommand {

	public SlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}
	
	public SlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, String ip, int port, ScheduledExecutorService scheduled) {
		super(clientPool, ip, port, scheduled);
	}

	public SlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, String ip, int port, String param, ScheduledExecutorService scheduled) {
		super(clientPool, ip, port, param, scheduled);
	}

	@Override
	public String getName() {
		return "slaveof";
	}
}
