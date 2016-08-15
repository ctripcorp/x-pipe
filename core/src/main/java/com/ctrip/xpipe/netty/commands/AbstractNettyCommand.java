package com.ctrip.xpipe.netty.commands;

import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandExecutionException;
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

	public AbstractNettyCommand(SimpleObjectPool<NettyClient> clientPool) {
		this.clientPool = clientPool;
	}

	public AbstractNettyCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(scheduled);
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
		}
	}

	protected abstract void doSendRequest(NettyClient nettyClient, ByteBuf byteBuf);

	protected abstract ByteBuf getRequest();
	

	@Override
	protected void doReset() {
		
	}
	
	public CommandFuture<V> future() {
		return future;
	}

}
