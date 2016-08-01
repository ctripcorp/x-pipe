package com.ctrip.xpipe.redis.meta.server.cluster.task;


import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SLOT_STATE;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.RemoteClusterServer;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * restart, do with unfinished tasks
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class ContinueResharding extends AbstractResharding{
	
	private Map<Integer, SlotInfo> slotInfos;
	
	public ContinueResharding(SlotManager slotManager, Map<Integer, SlotInfo> slotInfos, ClusterServers servers, ZkClient zkClient) {
		super(slotManager, servers, zkClient);
		this.slotInfos = slotInfos;

	}

	@Override
	protected void doExecute() throws Exception {
		
		logger.info("[doExecute]{}", slotInfos);
		
		for(Entry<Integer, SlotInfo> entry : slotInfos.entrySet()){
			
			Integer slot = entry.getKey();
			SlotInfo slotInfo = entry.getValue();
			
			if(slotInfo.getSlotState() != SLOT_STATE.MOVING){
				logger.warn("[doExecute][state not moving]{}", slotInfo);
				continue;
			}
			
			ClusterServer from = servers.getClusterServer(slotInfo.getServerId());
			if(from == null){
				from = new RemoteClusterServer(slotInfo.getServerId(), null);
			}
			ClusterServer to = servers.getClusterServer(slotInfo.getToServerId());
			if(to == null){
				to = new RemoteClusterServer(slotInfo.getToServerId(), null);
			}
			executeTask(new MoveSlotFromLiving(slot, from, to, zkClient));
		}
	}


	@Override
	protected void doReset() throws InterruptedException, ExecutionException {
		
	}


}
