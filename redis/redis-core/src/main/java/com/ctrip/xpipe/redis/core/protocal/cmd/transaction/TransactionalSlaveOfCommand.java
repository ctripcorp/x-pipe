package com.ctrip.xpipe.redis.core.protocal.cmd.transaction;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigRewrite;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.XSlaveofCommand;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * first try xslaveof, then slaveof
 * @Deprecated because redis may get inconsistent when using slaveof in transaction
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
@Deprecated
public class TransactionalSlaveOfCommand extends AbstractRedisCommand<Object[]>{
	
	private String ip;
	
	private int port;

	public TransactionalSlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, String ip, int port, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
		this.ip = ip;
		this.port = port;
	}
	
	@Override
	protected void doExecute() throws CommandExecutionException {

		logger.info("[doExecute][try xslaveof]{}", this);
		
		TransactionalCommand slaveofTransaction = new TransactionalCommand(getClientPool(), scheduled, new XSlaveofCommand(null, ip, port, scheduled), new ConfigRewrite(null, scheduled));
		try{
			slaveofTransaction.execute().addListener(new CommandFutureListener<Object[]>() {
				
				@Override
				public void operationComplete(CommandFuture<Object[]> commandFuture) throws Exception {
					if(!commandFuture.isSuccess()){
						failXslaveof(commandFuture.cause());
					}else{
						logger.info("[doExecute][xslaveof success]{}", this);
						future().setSuccess(commandFuture.get());
					}
				}
			});
		}catch(Exception e){
			failXslaveof(e);
		}
		
	}

	private void failXslaveof(Throwable e) {
		
		Throwable rootCause = ExceptionUtils.getRootCause(e);
		if((rootCause instanceof IOException)){
			logger.info("[failXslaveof][do not try slaveof]");
			fail(e);
			return;
		}
		
		logger.error("[doExecute][xlaveof fail, try slaveof]" + ip + ":"+ port, e);
		
		TransactionalCommand slaveofTransaction = new TransactionalCommand(getClientPool(), scheduled, new SlaveOfCommand(null, ip, port, scheduled), new ConfigRewrite(null, scheduled));
		try{
			slaveofTransaction.execute().addListener(new CommandFutureListener<Object[]>() {
				
				@Override
				public void operationComplete(CommandFuture<Object[]> commandFuture) throws Exception {
					
					if(!commandFuture.isSuccess()){
						fail(commandFuture.cause());
					}else{
						logger.info("[doExecute][slaveof success]{}", this);
						future().setSuccess(commandFuture.get());
					}
				}
			});
		}catch(Exception ex){
			fail(ex);
		}
	}

	@Override
	protected Object[] format(Object payload) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ByteBuf getRequest() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return String.format("TransactionalSlaveOfCommand: %s:%d", ip, port);
	}
}
