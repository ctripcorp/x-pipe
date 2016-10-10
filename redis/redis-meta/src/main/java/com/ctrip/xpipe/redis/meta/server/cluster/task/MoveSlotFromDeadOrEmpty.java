package com.ctrip.xpipe.redis.meta.server.cluster.task;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class MoveSlotFromDeadOrEmpty extends AbstractSlotMoveTask{
	
	public MoveSlotFromDeadOrEmpty(Integer slot, ClusterServer from, ClusterServer to, ZkClient zkClient) {
		super(slot, from, to, zkClient);
	}

	public MoveSlotFromDeadOrEmpty(Integer slot, ClusterServer to, ZkClient zkClient) {
		super(slot, null, to, zkClient);
	}


	@Override
	protected void doExecute() throws Exception {
		
		logger.info("[doExecute]{},{}->{}", slot, from, to);
		setSlotInfo(new SlotInfo(getTo().getServerId()));
		to.addSlot(slot);
		future().setSuccess(null);
	}

	@Override
	protected void doReset(){
		
	}

}
