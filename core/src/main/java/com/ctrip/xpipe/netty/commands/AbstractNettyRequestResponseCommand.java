package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public abstract class AbstractNettyRequestResponseCommand<V> extends AbstractNettyCommand<V> implements ByteBufReceiver, RequestResponseCommand<V>{
		
	protected ScheduledExecutorService scheduled;

	private ScheduledFuture<?> timeoutFuture = null;
	
	public AbstractNettyRequestResponseCommand(String host, int port, ScheduledExecutorService scheduled){
		super(host, port);
		this.scheduled = scheduled;
	}
	
	public AbstractNettyRequestResponseCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool);
		this.scheduled = scheduled;
	}
	
	@Override
	protected void doSendRequest(final NettyClient nettyClient, ByteBuf byteBuf) {
		
		if(logRequest()){
			getLogger().info("[doSendRequest]{}, {}", nettyClient, ByteBufUtils.readToString(byteBuf.slice()));
		}

		if(hasResponse()){
			nettyClient.sendRequest(byteBuf, this);
		}else{
			nettyClient.sendRequest(byteBuf);
			//TODO sendfuture, make sure send success
			future().setSuccess(null);
			return;
		}
		
		if(getCommandTimeoutMilli() > 0 && scheduled != null){
			int commandTimeoutMilli = getCommandTimeoutMilli();
			if (nettyClient instanceof RedisNettyClient && !((RedisNettyClient) nettyClient).getDoAfterConnectedOver()) {
				commandTimeoutMilli += ((RedisNettyClient) nettyClient).getAfterConnectCommandTimeoutMill();
			}

			getLogger().debug("[doSendRequest][schedule timeout]{}, {}", this, commandTimeoutMilli);
			int finalCommandTimeoutMilli = commandTimeoutMilli;
			timeoutFuture = scheduled.schedule(new AbstractExceptionLogTask() {
				
				@Override
				public void doRun() {
					getLogger().info("[{}][run][timeout]{}", AbstractNettyRequestResponseCommand.this, nettyClient);
					future().setFailure(new CommandTimeoutException("timeout " + finalCommandTimeoutMilli));
				}
			}, commandTimeoutMilli, TimeUnit.MILLISECONDS);
			
			future().addListener(new CommandFutureListener<V>() {

				@Override
				public void operationComplete(CommandFuture<V> commandFuture) {
					
					boolean cancel = true;
					try {
						commandFuture.get();
					} catch (InterruptedException e) {
					}catch(ExecutionException e){
						if(e.getCause() instanceof CommandTimeoutException){
							handleTimeout(nettyClient);
							cancel = false;
						}
					}
					if(cancel){
						getLogger().debug("[operationComplete][cancel timeout future]");
						cancelTimeout();
					}
				}
			});
			
		}
	}


	protected boolean hasResponse() {
		return true;
	}

	@Override
	public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
		
		if(future().isDone()){
			getLogger().debug("[receive][done, return]{}", channel);
			return RECEIVER_RESULT.ALREADY_FINISH;
		}
		
		try{
			 V result = doReceiveResponse(channel, byteBuf);
			 if(result != null){
				 if(logResponse()){
					 getLogger().info("[receive]{}, {}", ChannelUtil.getDesc(channel), result);
				 }
				 if(!future().isDone()) {
					 future().setSuccess(result);
				 }
			 }

			 if(result == null){
			 	return RECEIVER_RESULT.CONTINUE;
			 }
			 return RECEIVER_RESULT.SUCCESS;
		}catch(Exception e){
			future().setFailure(e);
			if(e instanceof ProtocalErrorResponse){
				return RECEIVER_RESULT.SUCCESS;
			}
		}
		return RECEIVER_RESULT.FAIL;
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
			future().setFailure(new SocketException("remote closed:" + nettyClient));
		}
	}

	@Override
	public void clientClosed(NettyClient nettyClient, Throwable th) {
		if(!future().isDone()){
			future().setFailure(new IOException("remote closed:" + nettyClient, th));
		}
	}

	protected abstract V doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception;

	@Override
	public int getCommandTimeoutMilli() {
		return 0;
	}

	protected void handleTimeout(NettyClient nettyClient) {
		nettyClient.onTimeout(AbstractNettyRequestResponseCommand.this, getCommandTimeoutMilli());
	}

	protected void cancelTimeout() {
		if (null != timeoutFuture && !timeoutFuture.isDone()) {
			timeoutFuture.cancel(false);
			timeoutFuture = null;
		}
	}
}
