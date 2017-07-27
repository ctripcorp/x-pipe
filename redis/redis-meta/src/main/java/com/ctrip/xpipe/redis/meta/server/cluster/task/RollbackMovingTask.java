package com.ctrip.xpipe.redis.meta.server.cluster.task;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Oct 10, 2016
 */
public class RollbackMovingTask extends AbstractSlotMoveTask{

	public RollbackMovingTask(Integer slot, ClusterServer from, ClusterServer to, ZkClient zkClient) {
		super(slot, from, to, zkClient);
	}

	@Override
	protected void doExecute() throws Exception {

		setSlotInfo(new SlotInfo(from.getServerId()));
		
		ClusterServer from = getFrom();
		ClusterServer to = getTo();
		if( from != null ){
			from.addSlot(slot);
		}
		if( to != null ){
			to.deleteSlot(slot);
		}
		future().setSuccess();
	}

	@Override
	protected void doReset() {
		
	}
}
