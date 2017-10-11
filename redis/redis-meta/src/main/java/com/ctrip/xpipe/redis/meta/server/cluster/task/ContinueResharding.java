package com.ctrip.xpipe.redis.meta.server.cluster.task;


import com.ctrip.xpipe.redis.meta.server.cluster.*;
import com.ctrip.xpipe.zk.ZkClient;

import java.util.Map;
import java.util.Map.Entry;

/**
 * restart, do with unfinished tasks
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class ContinueResharding extends AbstractResharding{
	
	private Map<Integer, SlotInfo> slotInfos;
	private RemoteClusterServerFactory<? extends ClusterServer>  remoteClusterServerFactory;
	
	public ContinueResharding(SlotManager slotManager, Map<Integer, SlotInfo> slotInfos, ClusterServers<?> servers, RemoteClusterServerFactory<?> remoteClusterServerFactory, ZkClient zkClient) {
		super(slotManager, servers, zkClient);
		this.slotInfos = slotInfos;
		this.remoteClusterServerFactory = remoteClusterServerFactory;

	}

	@Override
	protected void doShardingTask() throws ShardingException {
		
		logger.info("[doShardingTask]{}", slotInfos);
		
		for(Entry<Integer, SlotInfo> entry : slotInfos.entrySet()){
			
			Integer slot = entry.getKey();
			SlotInfo slotInfo = entry.getValue();
			
			if(slotInfo.getSlotState() != SLOT_STATE.MOVING){
				logger.warn("[doExecute][state not moving]{}", slotInfo);
				continue;
			}
			
			ClusterServer from = servers.getClusterServer(slotInfo.getServerId());
			ClusterServer to = servers.getClusterServer(slotInfo.getToServerId());
			if(to != null){
				if(from != null){
					executeTask(new MoveSlotFromLiving(slot, from, to, zkClient));
				}else{
					executeTask(new MoveSlotFromDeadOrEmpty(slot, remoteClusterServerFactory.createClusterServer(slotInfo.getServerId(), null), to, zkClient));
				}
			}else{
				executeTask(new RollbackMovingTask(slot, from, to, zkClient));
			}
		}
	}

	@Override
	protected void doReset(){
		
	}



}
