package com.ctrip.xpipe.redis.meta.server.cluster.task;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public interface SlotMoveTask extends Command<Void>{
	
	int getSlot();
	
	ClusterServer getFrom();
	
	ClusterServer getTo();
	
	

}
