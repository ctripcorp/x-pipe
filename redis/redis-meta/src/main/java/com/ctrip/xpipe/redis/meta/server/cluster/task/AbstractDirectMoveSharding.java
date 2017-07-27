package com.ctrip.xpipe.redis.meta.server.cluster.task;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * just move slots to the dest
 * @author wenchao.meng
 *
 * Jul 27, 2016
 */
public abstract class AbstractDirectMoveSharding extends AbstractResharding{

	public AbstractDirectMoveSharding(ExecutorService executors, SlotManager slotManager, ClusterServers<?> servers,
			ZkClient zkClient) {
		super(executors, slotManager, servers, zkClient);
	}

	public AbstractDirectMoveSharding(SlotManager slotManager, ClusterServers<?> servers, ZkClient zkClient) {
		super(slotManager, servers, zkClient);
	}

	
	@Override
	protected void doShardingTask() throws ShardingException {
		
		List<Integer> slots = getSlotsToArrange();
		
		List<ClusterServer> aliveServers = allAliveServers();
		logger.info("[doExecute][aliveServers]{}", aliveServers);
		if(aliveServers.size() == 0){
			logger.info("[doExecute][no aliveServers]{}");
			future().setSuccess(null);
			return;
		}

		int aliveTotal = getAliveTotal(aliveServers);
		
		int average = (aliveTotal + slots.size())/aliveServers.size();
		
		int slotIndex = 0;
		
		for(ClusterServer alive : aliveServers){
			
			int currentServerSlotsSize = slotManager.getSlotsSizeByServerId(alive.getServerId());
			for(int i=0 ; i<average - currentServerSlotsSize ; i++){
				executeTask(new MoveSlotFromDeadOrEmpty(slots.get(slotIndex), getDeadServer(), alive, zkClient));
				slotIndex++;
			}
		}
		
		int aliveServerIndex = 0;
		for(;slotIndex < slots.size(); slotIndex++){
			executeTask(
					new MoveSlotFromDeadOrEmpty(slots.get(slotIndex), 
							getDeadServer(), 
							aliveServers.get(aliveServerIndex++%aliveServers.size()), 
							zkClient)
				);
		}
	}

	protected abstract ClusterServer getDeadServer();

	protected List<ClusterServer> allAliveServers() {
		return new LinkedList<>(servers.allClusterServers());
	}

	protected abstract List<Integer> getSlotsToArrange();

	@Override
	public String getName() {
		return getClass().getSimpleName();
	}
}
