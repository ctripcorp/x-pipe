package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.cluster.SLOT_STATE;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
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
	
	@Autowired
	private MetaServerConfig config;
	
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	private Map<Integer, SlotInfo>  slotsMap = new ConcurrentHashMap<Integer,SlotInfo>();
	
	private Map<Integer, Set<Integer>> serverMap = new ConcurrentHashMap<>();
	
	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
		
	@Override
	protected void doStart() throws Exception {

		CuratorFramework client = zkClient.get();
		EnsurePath ensure = client.newNamespaceAwareEnsurePath(MetaZkConfig.getMetaServerSlotsPath());
		ensure.ensure(client.getZookeeperClient());

		refresh();
		
		scheduled.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				try{
					refresh();
				}catch(Throwable th){
					logger.error("[run]", th);
				}
			}
		}, config.getSlotRefreshMilli(), config.getSlotRefreshMilli(), TimeUnit.MILLISECONDS);
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
	public Set<Integer> getSlotsByServerId(int serverId) {
		try{
			lock.readLock().lock();
			return serverMap.get(serverId);
		}finally{
			lock.readLock().unlock();
		}
	}

	@Override
	public void refresh() throws Exception {
		
		doRefresh();
	}
	
	@Override
	public void refresh(int... slotIds) throws Exception {
		
		String slotsPath = MetaZkConfig.getMetaServerSlotsPath();
		CuratorFramework client = zkClient.get();

		Map<Integer, SlotInfo> slotsInfo = new HashMap<>();
		for(int slotId : slotIds){

			byte[] slotContent = client.getData().forPath(slotsPath + "/" + String.valueOf(slotId));
			SlotInfo slotInfo = Codec.DEFAULT.decode(slotContent, SlotInfo.class);
			slotsInfo.put(slotId, slotInfo);
		}
		
		try{
			lock.writeLock().lock();
			for(Entry<Integer, SlotInfo> entry : slotsInfo.entrySet()){
				Integer slotId = entry.getKey();
				SlotInfo slotInfo = entry.getValue();
				SlotInfo oldSlotInfo = slotsMap.get(slotId);

				slotsMap.put(slotId, slotInfo);
				
				if(oldSlotInfo != null){
					Set<Integer> oldServerSlot = serverMap.get(oldSlotInfo.getServerId());
					if(oldServerSlot != null){
						oldServerSlot.remove(slotId);
					}
				}
				getOrCreateServerMap(serverMap, slotInfo.getServerId()).add(slotId);
			}
		}finally{
			lock.writeLock().unlock();
		}
		
	}

	private void doRefresh() throws NumberFormatException, Exception {
		
		logger.info("[doRefresh]");
		
		Map<Integer, SlotInfo> slotsMap = new ConcurrentHashMap<>();
		Map<Integer, Set<Integer>> serverMap = new ConcurrentHashMap<>();
		
		CuratorFramework client = zkClient.get();
		String slotsPath = MetaZkConfig.getMetaServerSlotsPath();
		for(String slotPath : client.getChildren().forPath(slotsPath)){
			int slot = Integer.parseInt(slotPath);
			byte[] slotContent = client.getData().forPath(slotsPath + "/" + slotPath);
			SlotInfo slotInfo = Codec.DEFAULT.decode(slotContent, SlotInfo.class);

			
			Set<Integer> serverSlots = getOrCreateServerMap(serverMap, slotInfo.getServerId());			
			serverSlots.add(slot);
			slotsMap.put(slot, slotInfo);
		}
		
		try{
			lock.writeLock().lock();
			this.slotsMap = slotsMap;
			this.serverMap = serverMap;
		}finally{
			lock.writeLock().unlock();
		}
	}

	private Set<Integer> getOrCreateServerMap(Map<Integer, Set<Integer>> serverMap, int serverId) {

		return MapUtils.getOrCreate(serverMap, serverId, new ObjectFactory<Set<Integer>>() {

			@Override
			public Set<Integer> create() {
				return new HashSet<>();
			}
		});
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

	@Override
	public int getSlotsSizeByServerId(int serverId) {
		
		try{
			lock.readLock().lock();
			Set<Integer> slots = getSlotsByServerId(serverId);
			if(slots == null){
				return 0;
			}
			return slots.size();
		}finally{
			lock.readLock().unlock();
		}
	}

	@Override
	public Map<Integer, SlotInfo> allMoveingSlots() {
		
		try{
			lock.readLock().lock();
			
			Map<Integer, SlotInfo> result = new HashMap<>();
			
			for(Entry<Integer, SlotInfo> entry : slotsMap.entrySet()){
				
				Integer slot = entry.getKey();
				SlotInfo slotInfo = entry.getValue();
				
				if(slotInfo.getSlotState() == SLOT_STATE.MOVING){
					result.put(slot, slotInfo.clone());
				}
			}
			return result;
		} catch (CloneNotSupportedException e) {
			throw new IllegalStateException(e);
		}finally{
			lock.readLock().unlock();
		}
	}

	@Override
	public SlotInfo getSlotInfo(int slotId) {
		try{
			lock.readLock().lock();
			return slotsMap.get(slotId);
		}finally{
			lock.readLock().unlock();
		}
	}

	@Override
	public int getSlotIdByKey(Object key) {
		
		return key.hashCode()%TOTAL_SLOTS;
	}

	@Override
	public SlotInfo getSlotInfoByKey(Object key) {
		return slotsMap.get(getSlotIdByKey(key));
	}

	@Override
	public Integer getServerIdByKey(Object key) {
		
		int slotId = getSlotIdByKey(key);
		try{
			lock.readLock().lock();
			SlotInfo slotInfo = slotsMap.get(slotId);
			if(slotInfo == null){
				return null;
			}
			return slotInfo.getServerId();
		}finally{
			lock.readLock().unlock();
		}
	}

	@Override
	public Map<Integer, SlotInfo> allSlotsInfo() {
		return new HashMap<>(slotsMap);
	}

}
