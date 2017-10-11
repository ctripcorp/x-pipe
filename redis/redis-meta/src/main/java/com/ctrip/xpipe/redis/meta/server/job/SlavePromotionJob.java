package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public class SlavePromotionJob extends AbstractCommand<Void>{
	
	private KeeperMeta keeperMeta;
	private String promoteIp;
	private int promotePort;
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> keyedClientPool;
	private ScheduledExecutorService scheduled;
	
	public SlavePromotionJob(KeeperMeta keeperMeta, String promoteIp, int promotePort, SimpleKeyedObjectPool<InetSocketAddress, NettyClient> keyedClientPool, ScheduledExecutorService scheduled) {
		this.keeperMeta = keeperMeta;
		this.promoteIp = promoteIp;
		this.promotePort = promotePort;
		this.keyedClientPool = keyedClientPool;
		this.scheduled = scheduled;
	}

	@Override
	protected void doExecute() {
	
		XpipeThreadFactory.create("SLAVE_PROMOTION_JOB").newThread(new SlavePromotionTask(future())).start();
	}
	
	
	public class SlavePromotionTask implements Runnable, CommandFutureListener<String>{
		
		private CommandFuture<Void> future; 
		public SlavePromotionTask(CommandFuture<Void> future) {
			this.future = future;
		}
		@Override
		public void run() {
			
			logger.info("[run]{},{},{}", keeperMeta, promoteIp, promotePort);
			
			try{
				SimpleObjectPool<NettyClient> client = new XpipeObjectPoolFromKeyed<InetSocketAddress, NettyClient>(keyedClientPool, 
						new InetSocketAddress(keeperMeta.getIp(), keeperMeta.getPort()));
				
				SlaveOfCommand slaveOfCommand = new SlaveOfCommand(client, null, 0, String.format("%s %d", promoteIp, promotePort), scheduled);
				slaveOfCommand.execute().addListener(this);
				logger.info("[run][write cmd]{}", slaveOfCommand);
			} catch (Exception e) {
				logger.error("[run]" + keeperMeta + "," + promoteIp + ":" + promotePort, e);
				future.setFailure(e);
			}
		}
		
		@Override
		public void operationComplete(CommandFuture<String> commandFuture) throws Exception {

			logger.info("[operationComplete]{},{}", commandFuture);
			try{
				String result = commandFuture.get();
				if(result.equalsIgnoreCase(RedisProtocol.OK)){
					future.setSuccess(null);
				}else{
					future.setFailure(new IllegalStateException(result));
				}
				return;
			}catch(Exception e){
				logger.error("[onComplete]" + keeperMeta + "," + promoteIp + ":" + promotePort, e);
				future.setFailure(e);
			}
		}
	}

	@Override
	public String getName() {
		return String.format("slave promotion %s:%d", promoteIp, promotePort);
	}

	@Override
	protected void doReset(){
		throw new UnsupportedOperationException();
	}
}
