package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * @author wenchao.meng
 *
 *         Oct 28, 2016
 */
public class XSlaveofCommand extends AbstractSlaveOfCommand {

	public XSlaveofCommand(SimpleObjectPool<NettyClient> clientPool, String ip, int port) {
		super(clientPool, ip, port);
	}

	@Override
	public String getName() {
		return "xslaveof";
	}
}
