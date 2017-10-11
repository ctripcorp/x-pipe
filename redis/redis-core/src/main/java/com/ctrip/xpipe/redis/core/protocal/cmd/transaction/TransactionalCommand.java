package com.ctrip.xpipe.redis.core.protocal.cmd.transaction;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.pool.ReturnObjectException;
import com.ctrip.xpipe.redis.core.protocal.RedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
@SuppressWarnings("rawtypes")
public class TransactionalCommand extends AbstractRedisCommand<Object[]>{
	
	public static final String begin = "MULTI";
	
	public static final String end = "EXEC"; 
	
	private RedisCommand[]  commands;
	
	private SimpleObjectPool<NettyClient> parentClientPool;

	public TransactionalCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, RedisCommand ... commands) {
		super(clientPool, scheduled);
		this.commands = commands;
		this.parentClientPool = clientPool;
	}

	@Override
	protected void doExecute() throws CommandExecutionException {
		
		try{
			final NettyClient nettyClient = parentClientPool.borrowObject();
			SimpleObjectPool<NettyClient> clientPool = new FixedObjectPool<NettyClient>(nettyClient);
			
			startTransaction(clientPool);
			
			future().addListener(new CommandFutureListener<Object[]>() {
	
				@Override
				public void operationComplete(CommandFuture<Object[]> commandFuture) throws Exception {
					
					if(nettyClient != null){
						try {
							parentClientPool.returnObject(nettyClient);
						} catch (ReturnObjectException e) {
							logger.error("[doExecute]" + this, e);
						}
					}
				}
			});
		}catch(BorrowObjectException e){
			throw new CommandExecutionException("execute " + this, e);
		}
	}

	private void startTransaction(final SimpleObjectPool<NettyClient> clientPool) {

		logger.info("[startTransaction]{}", this);
		
		new MultiCommand(clientPool, scheduled).execute().addListener(new CommandFutureListener<String>() {

			@Override
			public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
				if(!commandFuture.isSuccess()){
					fail(commandFuture.cause());
				}else{
					doWork(clientPool);
				}
			}
		});
	}

	@SuppressWarnings("unchecked")
	protected void doWork(final SimpleObjectPool<NettyClient> clientPool) {
		
		SequenceCommandChain chain = new SequenceCommandChain(false);
		for(RedisCommand currentCommand : commands){
			OneTranscationCommand oneTranscationCommand = new OneTranscationCommand(clientPool, currentCommand, scheduled);
			chain.add(oneTranscationCommand);
		}
		
		chain.execute().addListener(new CommandFutureListener() {

			@Override
			public void operationComplete(CommandFuture commandFuture) throws Exception {
				
				if(!commandFuture.isSuccess()){
					logger.error("[doWork][fail]", commandFuture.cause());
				}
				
				endTranscation(clientPool);
			}
		});
	}

	protected void endTranscation(SimpleObjectPool<NettyClient> clientPool) {
		
		logger.info("[endTranscation]{}", this);
		
		new ExecCommand(clientPool, scheduled).execute().addListener(new CommandFutureListener<Object[]>() {
			
			@Override
			public void operationComplete(CommandFuture<Object[]> commandFuture) throws Exception {
				
				if(commandFuture.isSuccess()){
					future().setSuccess(commandFuture.get());
				}else{
					fail(commandFuture.cause());
				}
			}
		});
	}

	public static class MultiCommand extends AbstractRedisCommand<String>{

		public MultiCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
		}

		@Override
		protected String format(Object payload) {
			return payloadToString(payload);
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(TransactionalCommand.begin).format();
		}
	}

	public static class ExecCommand extends AbstractRedisCommand<Object[]>{

		public ExecCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
		}

		@Override
		protected Object[] format(Object payload) {
			if(payload instanceof Object[]){
				return (Object[])payload;
			}
			throw new IllegalStateException("expected array, but:" + payload.getClass() + "," + payload);
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(TransactionalCommand.end).format();
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
}
