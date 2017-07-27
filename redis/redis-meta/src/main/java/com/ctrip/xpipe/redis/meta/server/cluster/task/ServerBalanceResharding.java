package com.ctrip.xpipe.redis.meta.server.cluster.task;

import java.util.Set;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class ServerBalanceResharding extends AbstractResharding{

	public ServerBalanceResharding(SlotManager slotManager, ClusterServers<? extends ClusterServer> servers, ZkClient zkClient) {
		super(slotManager, servers, zkClient);
	}

	@Override
	protected void doShardingTask() throws ShardingException {
		
		Set<? extends ClusterServer> aliveServers = servers.allClusterServers();
		if(aliveServers.size() == 0){
			logger.info("[doExecute][no aliveServers]{}", aliveServers);
			future().setSuccess(null);
			return;
		}
		
		int totalSlots = getAliveTotal(aliveServers);
		int average = totalSlots/aliveServers.size();
		
		ClusterServer easyServer = null;
		for(ClusterServer clusterServer : aliveServers){
			
			int currentSlots = slotManager.getSlotsSizeByServerId(clusterServer.getServerId());
			logger.info("[doExecute]{}, {}", clusterServer, currentSlots);
			if(currentSlots < average/2){
				easyServer = clusterServer;
				break;
			}
		}
		
		if(easyServer == null){
			logger.info("[doExecute][no easy server][exit]{}", servers.allClusterServers());
			future().setSuccess(null);
			return;
		}
		
		logger.info("[doExecute][easy server]{}", easyServer);

		int maxMove = average - slotManager.getSlotsSizeByServerId(easyServer.getServerId());
		int totalMove = 0;
		for(ClusterServer clusterServer : aliveServers){
			if(clusterServer.equals(easyServer)){
				continue;
			}
			int currentSlotsSize = slotManager.getSlotsSizeByServerId(clusterServer.getServerId());
			int toMove = Math.min(currentSlotsSize - average, maxMove - totalMove); 
			if(toMove > 0){
				logger.info("[doExecute][move from server to dst]{}->{} {}", clusterServer, easyServer, toMove);
				move(clusterServer, easyServer, toMove);
			}
			
			totalMove += toMove;
			if(totalMove >= maxMove){
				logger.info("[doExecute][totalMove > maxMove]{} > {}", totalMove, maxMove);
				break;
			}
		}
	}


	protected void move(ClusterServer fromServer, ClusterServer toServer, int moveSize) {
		
		Set<Integer> slots = slotManager.getSlotsByServerId(fromServer.getServerId());
		int currentMove = 0;
		for(Integer slot : slots){
			if(currentMove >= moveSize){
				break;
			}
			executeTask(new MoveSlotFromLiving(slot, fromServer, toServer, zkClient));
			currentMove++;
		}
	}


}
