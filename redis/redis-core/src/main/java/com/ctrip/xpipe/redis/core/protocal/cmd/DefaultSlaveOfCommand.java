package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.command.UntilSuccess;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Feb 24, 2017
 */
public class DefaultSlaveOfCommand extends AbstractSlaveOfCommand{

	public DefaultSlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}
	
	public DefaultSlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, String ip, int port, ScheduledExecutorService scheduled){
		super(clientPool, ip, port, "", scheduled);
	}

	@Override
	protected void doExecute() throws CommandExecutionException {
		
		SimpleObjectPool<NettyClient> clientPool = getClientPool();
		
		UntilSuccess slaveOf = new UntilSuccess();
		slaveOf.add(new XSlaveofCommand(clientPool, ip, port, scheduled));
		slaveOf.add(new SlaveOfCommand(clientPool, ip, port, scheduled));
		
		SequenceCommandChain chain = new SequenceCommandChain(false);
		
		chain.add(slaveOf);
		chain.add(new ConfigRewrite(clientPool, scheduled));
		
		chain.execute().addListener(new CommandFutureListener<Object>() {
			
			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				
				if(commandFuture.isSuccess()){
					future().setSuccess(RedisClientProtocol.OK);
				}else{
					future().setFailure(commandFuture.cause());
				}
			}
		});
		
	}

	@Override
	protected String format(Object payload) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ByteBuf getRequest() {
		throw new UnsupportedOperationException();
	}
}
