package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.RemoteClusterServer;
import com.ctrip.xpipe.redis.meta.server.rest.ClusterApi;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.springframework.web.client.RestOperations;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class AbstractRemoteClusterServer extends AbstractClusterServer implements RemoteClusterServer{
	
	private int maxConnPerRoute = Integer.parseInt(System.getProperty("remoteMaxConnPerRoute", "10"));
	private int maxConnTotal = Integer.parseInt(System.getProperty("maxConnTotal", "100"));
	private int connectTimeout = Integer.parseInt(System.getProperty("remoteConnectTimeout", "5000"));
	private int soTimeout = Integer.parseInt(System.getProperty("remoteSoTimeout", "5000"));

	protected RestOperations restTemplate;
	
	private int currentServerId;
	
	private String httpHost;
	
	private String addSlotPath;
	
	private String deleteSlotPath;

	private String exportSlotPath;

	private String importSlotPath;
	
	private String notifySlotChangePath;

	public AbstractRemoteClusterServer(int currentServerId, int serverId) {
		this(currentServerId, serverId, null);
		
	}

	public AbstractRemoteClusterServer(int currentServerId, int serverId, ClusterServerInfo clusterServerInfo) {
		super(serverId, clusterServerInfo);
		
		this.currentServerId = currentServerId;
		
		if(clusterServerInfo != null){
			
			restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(maxConnPerRoute, maxConnTotal, connectTimeout, soTimeout);
			httpHost = String.format("http://%s:%d", clusterServerInfo.getIp(), clusterServerInfo.getPort());
			exportSlotPath = String.format("%s/%s/%s", httpHost, ClusterApi.PATH_PREFIX, ClusterApi.PATH_EXPORT_SLOT);
			importSlotPath = String.format("%s/%s/%s", httpHost, ClusterApi.PATH_PREFIX, ClusterApi.PATH_IMPORT_SLOT);
			addSlotPath = String.format("%s/%s/%s", httpHost, ClusterApi.PATH_PREFIX, ClusterApi.PATH_ADD_SLOT);
			deleteSlotPath = String.format("%s/%s/%s", httpHost, ClusterApi.PATH_PREFIX, ClusterApi.PATH_DELETE_SLOT);
			
			notifySlotChangePath = String.format("%s/%s/%s", httpHost, ClusterApi.PATH_PREFIX, ClusterApi.PATH_NOTIFY_SLOT_CHANGE);
		}
	}
	
	@Override
	public void notifySlotChange(int slotId) {
		new RemoteSlotCommand(notifySlotChangePath, slotId).execute();
	}


	@Override
	public CommandFuture<Void> addSlot(int slotId) {
		
		return new RemoteSlotCommand(addSlotPath, slotId).execute();
	}

	@Override
	public CommandFuture<Void> deleteSlot(int slotId) {
		return new RemoteSlotCommand(deleteSlotPath, slotId).execute();
	}

	@Override
	public int getCurrentServerId() {
		return currentServerId;
	}

	
	@Override
	public CommandFuture<Void> exportSlot(int slotId) {
		return new RemoteSlotCommand(exportSlotPath, slotId).execute();
	}

	@Override
	public CommandFuture<Void> importSlot(int slotId) {
		return new RemoteSlotCommand(importSlotPath, slotId).execute();
	}

	public String getHttpHost() {
		return httpHost;
	}
	
	class RemoteSlotCommand extends AbstractCommand<Void>{
		
		private String path;
		private int slotId;
		public RemoteSlotCommand(String path, int slotId) {
			this.path = path;
			this.slotId = slotId;
		}

		@Override
		public String getName() {
			return getClass().getSimpleName();
		}

		@Override
		protected void doExecute() throws Exception {
		
			restTemplate.postForObject(path, null, String.class, slotId);
			future().setSuccess();
		}

		@Override
		protected void doReset(){
			
		}
	}
}
