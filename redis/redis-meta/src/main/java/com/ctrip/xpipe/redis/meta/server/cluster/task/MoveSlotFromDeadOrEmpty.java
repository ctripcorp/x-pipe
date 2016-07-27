package com.ctrip.xpipe.redis.meta.server.cluster.task;

import java.util.concurrent.ExecutionException;

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
	public String getName() {
		return getClass().getSimpleName();
	}

	@Override
	protected void doExecute() throws Exception {
		
		setSlotInfo(new SlotInfo(getTo().getServerId()));
		future.setSuccess(null);
	}

	@Override
	protected void doReset() throws InterruptedException, ExecutionException {
		
	}

}
