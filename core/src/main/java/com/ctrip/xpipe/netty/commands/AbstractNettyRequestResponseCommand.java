package com.ctrip.xpipe.netty.commands;



import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.pool.XpipeNettyClientPool;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public abstract class AbstractNettyRequestResponseCommand<V> extends AbstractNettyCommand<V> implements ByteBufReceiver, RequestResponseCommand<V>{
	
	public AbstractNettyRequestResponseCommand(String host, int port){
		super(host, port);
	}
	
	public AbstractNettyRequestResponseCommand(SimpleObjectPool<NettyClient> clientPool) {
		super(clientPool);
	}

	public AbstractNettyRequestResponseCommand(XpipeNettyClientPool clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}
	
	@Override
	protected void doSendRequest(final NettyClient nettyClient, ByteBuf byteBuf) {
		
		if(logRequest()){
			logger.info("[doSendRequest]{}", this);
		}
		
		if(hasResponse()){
			nettyClient.sendRequest(byteBuf, this);
		}else{
			nettyClient.sendRequest(byteBuf);
			//TODO sendfuture, make sure send success
			future().setSuccess(null);
			return;
		}
		
		if(getCommandTimeoutMilli() > 0){
			
			logger.info("[doSendRequest][schedule timeout]{}, {}", this, getCommandTimeoutMilli());
			final ScheduledFuture<?> timeoutFuture = scheduled.schedule(new Runnable() {
				
				@Override
				public void run() {
					logger.info("[run][timeout]{}", nettyClient);
					future().setFailure(new CommandTimeoutException("timeout " +  + getCommandTimeoutMilli()));
				}
			}, getCommandTimeoutMilli(), TimeUnit.MILLISECONDS);
			
			future().addListener(new CommandFutureListener<V>() {

				@Override
				public void operationComplete(CommandFuture<V> commandFuture) {
					
					boolean cancel = true;
					try {
						commandFuture.get();
					} catch (InterruptedException e) {
					}catch(ExecutionException e){
						if(e.getCause() instanceof CommandTimeoutException){
							cancel = false;
						}
					}
					if(cancel){
						logger.info("[operationComplete][cancel timeout future]");
						timeoutFuture.cancel(false);
					}
				}
			});
			
		}
	}


	protected boolean hasResponse() {
		return true;
	}

	@Override
	public boolean receive(Channel channel, ByteBuf byteBuf) {
		
		if(future().isDone()){
			logger.debug("[receive][done, return]{}", channel);
			return true;
		}
		
		try{
			 V result = doReceiveResponse(channel, byteBuf);
			 if(result != null){
				 if(logResponse()){
					 logger.info("[receive]{}", result);
				 }
				 future().setSuccess(result);
			 }
			 return result != null;
		}catch(Exception e){
			future().setFailure(e);
		}
		return true;
	}
	

	protected boolean logRequest() {
		return true;
	}


	protected boolean logResponse() {
		return true;
	}

	@Override
	public void clientClosed(NettyClient nettyClient) {
		if(!future().isDone()){
			future().setFailure(new IOException("remote closed:" + nettyClient));
		}
	}

	protected abstract V doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception;

	@Override
	public int getCommandTimeoutMilli() {
		return 0;
	}
}
