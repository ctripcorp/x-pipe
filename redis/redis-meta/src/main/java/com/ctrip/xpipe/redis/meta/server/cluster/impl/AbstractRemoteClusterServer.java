package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import java.util.concurrent.ExecutionException;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.RemoteClusterServer;
import com.ctrip.xpipe.redis.meta.server.rest.ClusterApi;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class AbstractRemoteClusterServer extends AbstractClusterServer implements RemoteClusterServer{
	
	private int maxConnPerRoute = Integer.parseInt(System.getProperty("remoteMaxConnPerRoute", "5"));
	private int connectTimeout = Integer.parseInt(System.getProperty("remoteConnectTimeout", "5000"));
	private int soTimeout = Integer.parseInt(System.getProperty("remoteSoTimeout", "5000"));

	protected RestTemplate restTemplate;
	
	private int currentServerId;
	
	private String httpHost;
	
	private String notifySlotChangePath;

	private String exportSlotChangePath;

	private String importSlotChangePath;

	public AbstractRemoteClusterServer(int currentServerId, int serverId) {
		this(currentServerId, serverId, null);
		
	}

	public AbstractRemoteClusterServer(int currentServerId, int serverId, ClusterServerInfo clusterServerInfo) {
		super(serverId, clusterServerInfo);
		
		this.currentServerId = currentServerId;
		
		if(clusterServerInfo != null){
			HttpClient httpClient = HttpClientBuilder.create()
					.setMaxConnPerRoute(maxConnPerRoute)
					.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(soTimeout).build())
					.setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(connectTimeout).build())
					.build();
			ClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient); 
			restTemplate = new RestTemplate(factory);
			
			httpHost = String.format("http://%s:%d", clusterServerInfo.getIp(), clusterServerInfo.getPort());
			notifySlotChangePath = String.format("%s/%s/%s", httpHost, ClusterApi.PATH_FOR_CLUSTER, ClusterApi.PATH_NOTIFY_SLOT_CHANGE);
			exportSlotChangePath = String.format("%s/%s/%s", httpHost, ClusterApi.PATH_FOR_CLUSTER, ClusterApi.PATH_EXPORT_SLOT);
			importSlotChangePath = String.format("%s/%s/%s", httpHost, ClusterApi.PATH_FOR_CLUSTER, ClusterApi.PATH_IMPORT_SLOT);
		}
	}

	@Override
	public void notifySlotChange(int slotId) {
		restTemplate.postForObject(notifySlotChangePath, null, String.class, slotId);
	}

	@Override
	public int getCurrentServerId() {
		return currentServerId;
	}

	
	@Override
	public CommandFuture<Void> exportSlot(int slotId) {
		
		return new RemoteSlotCommand(exportSlotChangePath, slotId).execute();
	}

	@Override
	public CommandFuture<Void> importSlot(int slotId) {
		return new RemoteSlotCommand(importSlotChangePath, slotId).execute();
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
			future.setSuccess();
		}

		@Override
		protected void doReset() throws InterruptedException, ExecutionException {
			
		}
	}


}
