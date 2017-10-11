package com.ctrip.xpipe.redis.meta.server.cluster.task;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.zk.ZkClient;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
	protected void doExecute() throws Exception {

		try{
			logger.info("[doExecute]{},{}->{}", slot, from, to);
			//change slot info to moving
			SlotInfo slotInfo = new SlotInfo(from.getServerId());
			slotInfo.moveingSlot(to.getServerId());
			setSlotInfo(slotInfo);
			
			CommandFuture<Void> importFuture = to.importSlot(slot);
			
			if(importFuture.await(taskMaxWaitMilli, TimeUnit.MILLISECONDS)){
				if(!importFuture.isSuccess()){
					logger.info("[doExecute][import fail]", importFuture.cause());
					setFailAndlRollback(new Exception("import fail", importFuture.cause()));
					return;
				}
			}else{//timeout
				setFailAndlRollback(new TimeoutException("import time out " + to +"," + taskMaxWaitMilli));
				return;
			}
			
			CommandFuture<Void> exportFuture = from.exportSlot(slot);
			if(exportFuture.await(taskMaxWaitMilli, TimeUnit.MILLISECONDS)){
				if(!exportFuture.isSuccess()){
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

	private void setSuccess() throws ShardingException {
		
		logger.info("[setSuccess]{},{},{}", getSlot(), getFrom(), getTo());
		
		setSlotInfo(new SlotInfo(to.getServerId()));
		getTo().addSlot(slot);
		getFrom().deleteSlot(slot);
		future().setSuccess(null);
	}


	private void setFailAndlRollback(Throwable th) throws ShardingException {
		
		setSlotInfo(new SlotInfo(from.getServerId()));
		getFrom().addSlot(slot);
		getTo().deleteSlot(slot);
		future().setFailure(th);
	}


	@Override
	protected void doReset(){
		
	}
}
