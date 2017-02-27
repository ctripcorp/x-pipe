package com.ctrip.xpipe.redis.core.protocal.cmd;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;

import io.netty.buffer.ByteBuf;

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
		
		SequenceCommandChain slaveOf = new SequenceCommandChain(true);
		slaveOf.add(new XSlaveofCommand(clientPool, ip, port, scheduled));
		slaveOf.add(new SlaveOfCommand(clientPool, ip, port, scheduled));
		
		SequenceCommandChain chain = new SequenceCommandChain(false);
		
		chain.add(slaveOf);
		chain.add(new ConfigRewrite(clientPool, scheduled));
		
		chain.execute().addListener(new CommandFutureListener<List<CommandFuture<?>>>() {
			
			@Override
			public void operationComplete(CommandFuture<List<CommandFuture<?>>> commandFuture) throws Exception {
				
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
