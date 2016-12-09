package com.ctrip.xpipe.redis.core.protocal.cmd.transaction;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.pool.ReturnObjectException;
import com.ctrip.xpipe.redis.core.protocal.RedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;

import io.netty.buffer.ByteBuf;

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

	public TransactionalCommand(SimpleObjectPool<NettyClient> clientPool, RedisCommand ... commands) {
		super(clientPool);
		this.commands = commands;
		this.parentClientPool = clientPool;
	}

	@Override
	protected void doExecute() throws CommandExecutionException {
		
		NettyClient nettyClient = null;
		try{
			nettyClient = parentClientPool.borrowObject();
			SimpleObjectPool<NettyClient> clientPool = new FixedObjectPool<NettyClient>(nettyClient); 
			new MultiCommand(clientPool).execute().get();

			try{
				for(RedisCommand currentCommand : commands){
					OneTranscationCommand oneTranscationCommand = new OneTranscationCommand(clientPool, currentCommand);
					oneTranscationCommand.execute().get();
				}
			}catch(Exception e){
				logger.error("[doExecute]", e);
			}
			//submit even some command may fail
			new ExecCommand(clientPool).execute().addListener(new CommandFutureListener<Object[]>() {
				
				@Override
				public void operationComplete(CommandFuture<Object[]> commandFuture) throws Exception {
					if(commandFuture.isSuccess()){
						future().setSuccess(commandFuture.get());
					}else{
						future().setFailure(commandFuture.cause());
					}
				}
			});
		} catch (Exception e) {
			future().setFailure(e);
		} finally{
			if(nettyClient != null){
				try {
					parentClientPool.returnObject(nettyClient);
				} catch (ReturnObjectException e) {
					logger.error("[doExecute]" + this, e);
				}
			}
		}
	}

	
	public static class MultiCommand extends AbstractRedisCommand<String>{

		public MultiCommand(SimpleObjectPool<NettyClient> clientPool) {
			super(clientPool);
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

		public ExecCommand(SimpleObjectPool<NettyClient> clientPool) {
			super(clientPool);
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
