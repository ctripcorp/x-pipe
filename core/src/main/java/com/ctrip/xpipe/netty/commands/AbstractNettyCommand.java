package com.ctrip.xpipe.netty.commands;

import java.net.InetSocketAddress;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.pool.ReturnObjectException;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public abstract class AbstractNettyCommand<V> extends AbstractCommand<V>{
	
	private SimpleObjectPool<NettyClient> clientPool;
	
	private volatile boolean poolCreated = false;
	
	private String host;
	
	private int port;

	public AbstractNettyCommand(String host, int port){
		this(NettyPoolUtil.createNettyPool(new InetSocketAddress(host, port)));
		poolCreated = true;
		this.host = host;
		this.port = port;
	}

	public AbstractNettyCommand(SimpleObjectPool<NettyClient> clientPool) {
		this.clientPool = clientPool;
	}

	@Override
	protected void doExecute() throws CommandExecutionException {
		
		NettyClient nettyClient = null;
		try {
			logger.debug("[doExecute]{}", this);
			nettyClient = clientPool.borrowObject();
			ByteBuf byteBuf = getRequest();
			doSendRequest(nettyClient, byteBuf);
		} catch (BorrowObjectException e) {
			throw new CommandExecutionException("execute " + this, e);
		}finally{

			if( nettyClient != null){
				try {
					clientPool.returnObject(nettyClient);
				} catch (ReturnObjectException e) {
					logger.error("[doExecute]", e);
				}
			}

			if(poolCreated){
				future().addListener(new CommandFutureListener<V>() {

					@Override
					public void operationComplete(CommandFuture<V> commandFuture) throws Exception {
						LifecycleHelper.stopIfPossible(clientPool);
						LifecycleHelper.disposeIfPossible(clientPool);
					}
				});
			}
			
		}
	}

	protected abstract void doSendRequest(NettyClient nettyClient, ByteBuf byteBuf);

	public abstract ByteBuf getRequest();
	

	@Override
	protected void doReset() {
		if(poolCreated){
			this.clientPool = NettyPoolUtil.createNettyPool(new InetSocketAddress(host, port)); 
		}
	}
	
	protected SimpleObjectPool<NettyClient> getClientPool() {
		return clientPool;
	}

	@Override
	public String toString() {
		return String.format("T:%s, CMD:%s", clientPool == null? "null": clientPool.desc(), getName());
	}
}
