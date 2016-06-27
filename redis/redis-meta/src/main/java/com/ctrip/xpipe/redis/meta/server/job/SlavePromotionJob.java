package com.ctrip.xpipe.redis.meta.server.job;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;

import com.ctrip.xpipe.api.job.JobFuture;
import com.ctrip.xpipe.job.AbstractJob;
import com.ctrip.xpipe.job.DefaultJobFuture;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.client.Client;
import com.ctrip.xpipe.redis.core.client.ClientPool;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.CmdContext;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.RequestResponseCommandListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public class SlavePromotionJob extends AbstractJob<Void>{
	
	private KeeperMeta keeperMeta;
	private String promoteIp;
	private int promotePort;
	
	public SlavePromotionJob(KeeperMeta keeperMeta, String promoteIp, int promotePort) {
		this.keeperMeta = keeperMeta;
		this.promoteIp = promoteIp;
		this.promotePort = promotePort;
	}

	@Override
	protected JobFuture<Void> doExecute() {
	
		JobFuture<Void> future = new DefaultJobFuture<>();
		XpipeThreadFactory.create("SLAVE_PROMOTION_JOB").newThread(new SlavePromotionTask(future)).start();
		return future;
	}
	
	
	public class SlavePromotionTask implements Runnable, RequestResponseCommandListener{
		
		private JobFuture<Void> future; 
		public SlavePromotionTask(JobFuture<Void> future ) {
			this.future = future;
		}
		@Override
		public void run() {
			
			logger.info("[run]{},{},{}", keeperMeta, promoteIp, promotePort);
			
			ClientPool clientPool = ClientPool.getInstance();
			
			try{
				Client client = clientPool.getClient(new InetSocketAddress(keeperMeta.getIp(), keeperMeta.getPort()));
				SlaveOfCommand slaveOfCommand = new SlaveOfCommand(null, 0, String.format("%s %d", promoteIp, promotePort));
				slaveOfCommand.setCommandListener(this);
				logger.info("[run][write cmd]{}", slaveOfCommand);
				client.sendCommand(slaveOfCommand);
			} catch (Exception e) {
				logger.error("[run]" + keeperMeta + "," + promoteIp + ":" + promotePort, e);
				future.setFailure(e);
			}
		}
		
		@Override
		public void onComplete(CmdContext cmdContext, Object data, Exception e) {
			
			logger.info("[onComplete]{},{}", data, e);
			if(e != null){
				logger.error("[onComplete]" + keeperMeta + "," + promoteIp + ":" + promotePort, e);
				future.setFailure(e);
				return;
			}
			
			String result = null;
			if(data instanceof String){
				result = (String) data;
			}else if(data instanceof ByteArrayOutputStreamPayload){
				result = new String(((ByteArrayOutputStreamPayload)data).getBytes());
				logger.info("[onComplete]{}", result);
			}
			if(result != null){
				if(result.equalsIgnoreCase(RedisProtocol.OK)){
					future.setSuccess(null);
				}else{
					future.setFailure(new IllegalStateException(result));
				}
				return;
			}
			logger.info("[onComplete][unknown data]{}", data);
			future.setFailure(new IllegalStateException(String.format("%s( %s)", data, data == null?null:data.getClass())));
		}
	}

	protected String readLine(InputStream ins) throws IOException {
		
		StringBuilder sb = new StringBuilder();
		int last = 0;
		while(true){
			
			int data = ins.read();
			if(data == -1){
				return null;
			}
			sb.append((char)data);
			if(data == '\n' && last == '\r'){
				break;
			}
			last = data;
		}
		
		return sb.toString();
	}
}
