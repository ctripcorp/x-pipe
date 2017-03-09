package com.ctrip.xpipe.redis.keeper.server;

import java.net.Socket;

import com.ctrip.xpipe.redis.core.server.AbstractRedisAction;


/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午3:08:46
 */
public abstract class AbstractRedisSlaveAction extends AbstractRedisAction {

	public AbstractRedisSlaveAction(Socket socket) {
		super(socket);
	}
	
}