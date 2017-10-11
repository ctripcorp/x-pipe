package com.ctrip.xpipe.redis.keeper.server;

import com.ctrip.xpipe.redis.core.server.AbstractRedisAction;

import java.net.Socket;


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