package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * @author marsqing
 *
 *         May 9, 2016 3:19:37 PM
 */
public class SlaveOfCommand extends AbstractSlaveOfCommand {

	public SlaveOfCommand(SimpleObjectPool<NettyClient> clientPool) {
		super(clientPool);
	}
	
	public SlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, String ip, int port) {
		super(clientPool, ip, port);
	}

	public SlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, String ip, int port, String param) {
		super(clientPool, ip, port, param);
	}

	@Override
	public String getName() {
		return "slaveof";
	}
}
