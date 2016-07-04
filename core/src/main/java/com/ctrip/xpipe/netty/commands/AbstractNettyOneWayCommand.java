package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.pool.XpipeObjectPool;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public abstract class AbstractNettyOneWayCommand<V> extends AbstractNettyCommand<V>{
	
	public AbstractNettyOneWayCommand(XpipeObjectPool<NettyClient> clientPool) {
		super(clientPool);
	}

	@Override
	protected void doSendRequest(NettyClient nettyClient, ByteBuf byteBuf) {
		nettyClient.sendRequest(byteBuf);
	}

}
