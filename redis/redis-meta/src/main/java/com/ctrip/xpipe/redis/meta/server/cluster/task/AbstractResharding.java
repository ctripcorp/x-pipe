package com.ctrip.xpipe.redis.meta.server.cluster.task;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.monitor.CatUtils;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public abstract class AbstractResharding extends AbstractCommand<Void> implements ReshardingTask{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private static String MONITOR_NAME = "resharding";  
	
	protected ExecutorService executors;
	
	protected SlotManager slotManager;
	protected ClusterServers<? extends ClusterServer> servers;
	protected ZkClient zkClient;
	
	private volatile boolean allTaskSubmited = false;
	private AtomicInteger totalTasks = new AtomicInteger();
	private AtomicInteger completedTasks = new AtomicInteger();

	public AbstractResharding(ExecutorService executors, SlotManager slotManager, ClusterServers<?> servers, ZkClient zkClient) {
		this.executors = executors;
		this.slotManager = slotManager;
		this.servers = servers;
		this.zkClient = zkClient;

	}

	public AbstractResharding(SlotManager slotManager, ClusterServers<?> servers, ZkClient zkClient) {
		this(Executors.newCachedThreadPool(XpipeThreadFactory.create("Sharding")), slotManager, servers, zkClient);
	}

	@Override
	protected void doExecute() throws Exception {
		
		CatUtils.newFutureTaskTransaction(MONITOR_NAME, getName(), future());
		
		slotManager.refresh();
		
		doShardingTask();
		allTaskSubmited();
	}
	

	protected abstract void doShardingTask() throws ShardingException;

	protected void notifyAliveServers(int slotId) {
		
		for(ClusterServer clusterServer: servers.allClusterServers()){
			clusterServer.notifySlotChange(slotId);
		}
	}

	@Override
	public String getName() {
		return getClass().getSimpleName();
	}

	protected int getAliveTotal(Collection<? extends ClusterServer> aliveServers) {
		
		int result = 0;
		for(ClusterServer server : aliveServers){
			result += slotManager.getSlotsSizeByServerId(server.getServerId());
		}
		return result;
	}

	protected void executeTask(SlotMoveTask task) {
		
		totalTasks.incrementAndGet();
		
		CatUtils.newFutureTaskTransaction(MONITOR_NAME + "-" + getName(), task.toString(), task.future());
		
		task.execute(executors).addListener(new MoveListener());
	}

	public class MoveListener implements CommandFutureListener<Void>{

		@Override
		public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {

			completedTasks.incrementAndGet();
			checkTaskFinish();
			
			if(commandFuture.isSuccess()){
				
				SlotMoveTask task = (SlotMoveTask) commandFuture.command();
				notifyAliveServers(task.getSlot());
			}else{
				logger.error("[error]" + commandFuture.command(), commandFuture.cause());
			}
			//if fail leave it to leader redo
		}
	}
	
	protected void allTaskSubmited() {
		
		allTaskSubmited = true;
		checkTaskFinish();
	}

	public void checkTaskFinish() {
		
		if(allTaskSubmited && completedTasks.get() >= totalTasks.get()){
			logger.info("[checkTaskFinish][finish]");
			future().setSuccess(null);
		}
	}
	
	@Override
	protected void doReset(){
		throw new UnsupportedOperationException();
	}
}
