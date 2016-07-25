package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
@Component
public class DefaultSlotManager extends AbstractLifecycle implements SlotManager, TopElement{
	

	@Autowired
	private ZkClient zkClient;
	
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	private Map<Integer, SlotInfo>  slotsMap = new ConcurrentHashMap<Integer,SlotInfo>();
	
	private Map<Integer, Set<Integer>> serverMap = new ConcurrentHashMap<>();
		
	@Override
	protected void doStart() throws Exception {
		refresh();
	}

	@Override
	public Integer getSlotServerId(int slotId) {

		try{
			lock.readLock().lock();
			
			SlotInfo  slotInfo = slotsMap.get(slotId);
			if(slotInfo == null){
				return null;
			}
			return slotInfo.getServerId();
		}finally{
			lock.readLock().unlock();
		}
	}

	@Override
	public Set<Integer> getByServerId(int serverId) {
		try{
			lock.readLock().lock();
			return serverMap.get(serverId);
		}finally{
			lock.readLock().unlock();
		}
	}

	@Override
	public void refresh() throws Exception {
		
		Map<Integer, SlotInfo> result = new ConcurrentHashMap<>();
		Map<Integer, Set<Integer>> server = new ConcurrentHashMap<>();
		
		CuratorFramework client = zkClient.get();
		String slotsPath = MetaZkConfig.getMetaServerSlotsPath();
		for(String slotPath : client.getChildren().forPath(slotsPath)){
			int slot = Integer.parseInt(slotPath);
			byte[] slotContent = client.getData().forPath(slotsPath + "/" + slotPath);
			SlotInfo slotInfo = Codec.DEFAULT.decode(slotContent, SlotInfo.class);

			
			Set<Integer> serverMap = MapUtils.getOrCreate(server, slotInfo.getServerId(), new ObjectFactory<Set<Integer>>() {

				@Override
				public Set<Integer> create() {
					return new HashSet<>();
				}
			});
			
			serverMap.add(slot);
			result.put(slot, slotInfo);
		}
		
		try{
			lock.writeLock().lock();
			slotsMap = result;
			serverMap = server;
		}finally{
			lock.writeLock().unlock();
		}
	}

	@Override
	public int getOrder() {
		return 0;
	}

	@Override
	public void move(int slotId, int fromServer, int toServer) {
		
		
		try{
			lock.writeLock().lock();
			
			if(serverMap.get(fromServer) == null){
				logger.error("[fromServer not Found]" + fromServer);
				return;
			}
			slotsMap.put(slotId, new SlotInfo(toServer));
			serverMap.get(fromServer).remove(slotId);
			
			Set<Integer> toSlots = MapUtils.getOrCreate(serverMap, toServer, new ObjectFactory<Set<Integer>>() {

				@Override
				public Set<Integer> create() {
					return new HashSet<>();
				}
			});
			
			toSlots.add(slotId);
		}finally{
			lock.writeLock().unlock();
		}
	}

	@Override
	public Set<Integer> allSlots() {
		try{
			lock.readLock().lock();
			return new HashSet<>(slotsMap.keySet());
		}finally{
			lock.readLock().unlock();
		}
	}
	
	@Override
	public Set<Integer> allServers() {
		
		try{
			lock.readLock().lock();
			return new HashSet<>(serverMap.keySet());
		}finally{
			lock.readLock().unlock();
		}
	}

}
