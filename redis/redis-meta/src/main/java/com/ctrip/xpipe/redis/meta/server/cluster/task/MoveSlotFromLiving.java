package com.ctrip.xpipe.redis.meta.server.cluster.task;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class MoveSlotFromLiving extends AbstractSlotMoveTask{
	
	public MoveSlotFromLiving(Integer slot, ClusterServer from, ClusterServer to, ZkClient zkClient) {
		super(slot, from, to, zkClient);
	}


	@Override
	public String getName() {
		return getClass().getSimpleName();
	}


	@Override
	protected void doExecute() throws Exception {

		try{
			logger.info("[doExecute]{},{}->{}", slot, from, to);
			//change slot info to moving
			SlotInfo slotInfo = new SlotInfo(getFrom().getServerId());
			slotInfo.moveingSlot(getTo().getServerId());
			setSlotInfo(slotInfo);
			
			CommandFuture<Void> importFuture = to.importSlot(slot);
			
			if(importFuture.await(taskMaxWaitMilli, TimeUnit.MILLISECONDS)){
				if(!future.isSuccess()){
					logger.info("[doExecute][import fail]", importFuture.cause());
					setFailAndlRollback(new Exception("import fail", importFuture.cause()));
					return;
				}
			}else{//timeout
				setFailAndlRollback(new TimeoutException("import time out " + to +"," + taskMaxWaitMilli));
				return;
			}
			
			CommandFuture<Void> exportFuture = from.exportSlot(slot);
			if(importFuture.await(taskMaxWaitMilli, TimeUnit.MILLISECONDS)){
				if(!future.isSuccess()){
					logger.info("[doExecute][export fail]", exportFuture.cause());
					setFailAndlRollback(new Exception("export fail", exportFuture.cause()));
					return;
				}
			}else{
				setFailAndlRollback(new TimeoutException("export time out " + from + "," + taskMaxWaitMilli));
				return;
			}
			
			setSuccess();
		}catch(Throwable th){
			setFailAndlRollback(th);
		}
	}

	private void setSuccess() throws Exception {
		
		future.setSuccess(null);
		setSlotInfo(new SlotInfo(to.getServerId()));
	}


	private void setFailAndlRollback(Throwable th) throws Exception {
		
		future.setFailure(th);
		setSlotInfo(new SlotInfo(from.getServerId()));
	}


	@Override
	protected void doReset() throws InterruptedException, ExecutionException {
		
	}
}
