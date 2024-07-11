package com.ctrip.xpipe.redis.meta.server.cluster.impl;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.cluster.*;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public class DefaultCurrentClusterServer extends AbstractClusterServer implements CurrentClusterServer, TopElement{

	@Autowired
	private ZkClient zkClient;
	
	@Autowired
	private MetaServerConfig config;
	
	@Autowired
	private SlotManager slotManager;
	
	@Autowired
	private MetaServerLeaderElector metaserverLeaderElector;
	
	private int currentServerId;
	
	private String serverPath;

	@Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
	private Executor executors;
	
	private PersistentNode persistentNode;
	
	public DefaultCurrentClusterServer() {
		
	}

	@Override
	protected void doInitialize() throws Exception {

		this.currentServerId = config.getMetaServerId();
		serverPath = MetaZkConfig.getMetaServerRegisterPath() + "/" + currentServerId;
		
		setServerId(currentServerId);
		setClusterServerInfo(new ClusterServerInfo(config.getMetaServerIp(), config.getMetaServerPort()));
	}
	
	@Override
	protected void doStart() throws Exception {
		
		CuratorFramework client = zkClient.get();
		
		if(client.checkExists().forPath(serverPath) != null){ 
			
			byte []data = client.getData().forPath(serverPath);
			throw new IllegalStateException("server already exist:" + new String(data));
		}

		persistentNode = new PersistentNode(zkClient.get(), CreateMode.EPHEMERAL, false, serverPath, Codec.DEFAULT.encodeAsBytes(getClusterInfo()));
		persistentNode.start();
	}

	@Override
	public int getOrder() {
		return ORDER;
	}

	@Override
	protected void doStop() throws Exception {
		persistentNode.close();
	}


	@Override
	protected void doDispose() throws Exception {
		super.doDispose();
	}

	public void setZkClient(ZkClient zkClient) {
		this.zkClient = zkClient;
	}

	public void setConfig(MetaServerConfig config) {
		this.config = config;
	}

	@Override
	public void notifySlotChange(int slotId) {
		new SlotRefreshCommand(slotId).execute(executors);
	}
	
	
	@Override
	public CommandFuture<Void> addSlot(int slotId) {
		return new SlotAddCommand(slotId).execute(executors);
	}

	@Override
	public CommandFuture<Void> deleteSlot(int slotId) {
		return new SlotDeleteCommand(slotId).execute(executors);
	}

	@Override
	public CommandFuture<Void> exportSlot(int slotId) {

		return new SlotExportCommand(slotId).execute(executors);
	}

	@Override
	public CommandFuture<Void> importSlot(int slotId) {
		return new SlotImportCommand(slotId).execute(executors);
	}

	@Override
	public Set<Integer> slots() {
		Set<Integer> slots = slotManager.getSlotsByServerId(currentServerId);
		if(slots == null){
			return new HashSet<>();
		}
		return slots; 
	}
	
	@Override
	public boolean isLeader() {
		return metaserverLeaderElector.amILeader();
	}
	
	
	protected boolean isExporting(Object key){
		
		SlotInfo slotInfo = slotManager.getSlotInfoByKey(key);
		if(slotInfo.getSlotState() == SLOT_STATE.MOVING){
			return true;
		}
		return false;
	}
	
	public void doWaitForSlotCommandsFinish() {
		//TODO wait for slot to clean export info
	}

	@Override
	public boolean hasKey(Object key) {
		
		Integer serverId = slotManager.getServerIdByKey(key);
		if(serverId == null){
			return false;
		}
		return serverId == this.getServerId();
	}


	class SlotImportCommand extends AbstractCommand<Void>{
		
		private int slotId;
		public SlotImportCommand(int slotId){
			this.slotId = slotId;
		}

		@Override
		public String getName() {
			return "SlotImport";
		}

		@Override
		protected void doExecute() throws Exception {
			
			slotManager.refresh(slotId);
			SlotInfo slotInfo = slotManager.getSlotInfo(slotId);
			if(slotInfo.getSlotState() == SLOT_STATE.MOVING && slotInfo.getToServerId() == getServerId()){
				logger.info("[doExecute][import({})]{}, {}", currentServerId, slotId, slotInfo);
			}else{
				throw new IllegalStateException("error import " + slotId + "," + slotInfo);
			}
			doSlotImport(slotId);
			future().setSuccess();
		}
		@Override
		protected void doReset() {
			
		}
	}
	
	class SlotExportCommand extends AbstractCommand<Void>{
		
		private int slotId;
		public SlotExportCommand(int slotId){
			this.slotId = slotId;
		}

		@Override
		public String getName() {
			return "SlotExport";
		}

		@Override
		protected void doExecute() throws Exception {
			
			slotManager.refresh(slotId);
			SlotInfo slotInfo = slotManager.getSlotInfo(slotId);
			if(slotInfo.getSlotState() == SLOT_STATE.MOVING && slotInfo.getServerId() == getServerId()){
				logger.info("[doExecute][export({}){}, {},{}", currentServerId, slotId, slotInfo, getServerId());
			}else{
				throw new IllegalStateException("error export " + slotId + "," + slotInfo);
			}
			doSlotExport(slotId);
			future().setSuccess();
		}
		@Override
		protected void doReset() {
			
		}
	}

	class SlotAddCommand extends AbstractCommand<Void>{
		
		private int slotId;
		public SlotAddCommand(int slotId) {
			this.slotId = slotId;
		}

		@Override
		public String getName() {
			return "SlotRefreshCommand";
		}

		@Override
		protected void doExecute() throws Exception {
			
			slotManager.refresh(slotId);
			SlotInfo slotInfo = slotManager.getSlotInfo(slotId);
			if(slotInfo.getSlotState() == SLOT_STATE.NORMAL && slotInfo.getServerId() == getServerId()){
				logger.info("[doExecute][slot add]{}, {}", slotId, slotInfo);
			}else{
				throw new IllegalStateException("error add " + slotId + "," + slotInfo);
			}
			doSlotAdd(slotId);
			future().setSuccess();
		}

		@Override
		protected void doReset(){
		}
	} 

	class SlotDeleteCommand extends AbstractCommand<Void>{
		
		private int slotId;
		public SlotDeleteCommand(int slotId) {
			this.slotId = slotId;
		}

		@Override
		public String getName() {
			return "SlotRefreshCommand";
		}

		@Override
		protected void doExecute() throws Exception {
			
			slotManager.refresh(slotId);
			SlotInfo slotInfo = slotManager.getSlotInfo(slotId);
			if(slotInfo.getSlotState() == SLOT_STATE.NORMAL && slotInfo.getServerId() != getServerId()){
				logger.info("[doExecute][slot delete]{}, {}", slotId, slotInfo);
			}else{
				throw new IllegalStateException("error delete " + slotId + "," + slotInfo);
			}
			doSlotDelete(slotId);
			future().setSuccess();
		}
		@Override
		protected void doReset(){
		}
	} 

	class SlotRefreshCommand extends AbstractCommand<Void>{
		
		private int slotId;
		public SlotRefreshCommand(int slotId) {
			this.slotId = slotId;
		}

		@Override
		public String getName() {
			return "SlotRefreshCommand";
		}

		@Override
		protected void doExecute() throws Exception {
			
			slotManager.refresh(slotId);
			future().setSuccess();
		}
		@Override
		protected void doReset(){
		}
	}

	protected void doSlotImport(int slotId) {
	}

	protected void doSlotAdd(int slotId) {
	}

	protected  void doSlotExport(int slotId) {
	} 

	protected void doSlotDelete(int slotId) {
	}
}
