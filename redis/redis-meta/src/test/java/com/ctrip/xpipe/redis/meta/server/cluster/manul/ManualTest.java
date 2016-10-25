package com.ctrip.xpipe.redis.meta.server.cluster.manul;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestTemplate;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerService;
import com.ctrip.xpipe.redis.meta.server.TestMetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.AbstractMetaServerClusterTest;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.rest.ClusterApi;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardType;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class ManualTest extends AbstractMetaServerClusterTest{
	
	private int []serverPorts = new int[]{9747, 9748, 9749};
	private String clusterId = "cluster1";
	private String shardId = "shard1";
	private RestTemplate template = new RestTemplate();

	
	@Before
	public void beforeManualTest(){
		System.setProperty(ArrangeTaskExecutor.ARRANGE_TASK_EXECUTOR_START, "true");
		
	}
	
	@Test
	public void startServers() throws Exception{
		
		createMetaServers(1);

		waitForAnyKeyToExit();
	}

	
	
	@Test
	public void sendPing(){
		
		for(int serverPort : serverPorts){
			doSendPing(serverPort);
		}
	}

	private void doSendPing(int serverPort) {
		
		RestTemplate template = new RestTemplate();
		KeeperMeta keeperMeta = new KeeperMeta();
		String address = String.format("http://localhost:%d/%s/%s", serverPort, MetaServerKeeperService.PATH_PREFIX, MetaServerKeeperService.PATH_PING);
		KeeperInstanceMeta kim = new KeeperInstanceMeta(clusterId, shardId, keeperMeta);
		template.postForObject(address, kim, String.class, clusterId, shardId);
	}

	@Test
	public void pingMoving() throws Exception{
		
		int currentServerId = 2, toServerId = 3;
		int slotId = clusterId.hashCode()%TestMetaServer.total_slots;
		logger.info("[pingMoving]{}", slotId);

		CuratorFramework client = getCuratorFramework(2181);
		
		SlotInfo info = new SlotInfo(currentServerId);
		info.moveingSlot(toServerId);
		client.setData().forPath(MetaZkConfig.getMetaServerSlotsPath() + "/" + slotId, Codec.DEFAULT.encodeAsBytes(info));

		
		refreshServers();
		waitForAnyKeyToExit();
		doSendPing(serverPorts[currentServerId - 1]);
	}

	private void refreshServers() {

		for(int serverPort : serverPorts){
			String address = String.format("http://localhost:%d/%s/%s", serverPort, ClusterApi.PATH_PREFIX, "refresh");
			logger.info("[refreshServers]{}", address);
			template.postForObject(address, null, String.class);
		}
		
	}

	@Test
	public void pingCircular(){


		KeeperInstanceMeta keeperInstanceMeta = new KeeperInstanceMeta(clusterId, shardId, new KeeperMeta());

		int serverId = 1;
		for(int serverPort : serverPorts){

			ForwardInfo forwardInfo = new ForwardInfo(ForwardType.FORWARD);
			forwardInfo.addForwardServers(serverId);	

			HttpHeaders headers = new HttpHeaders();
			headers.add(MetaServerService.HTTP_HEADER_FOWRARD, Codec.DEFAULT.encode(forwardInfo));
			HttpEntity<KeeperInstanceMeta> entity = new HttpEntity<KeeperInstanceMeta>(keeperInstanceMeta, headers);

			String address = String.format("http://localhost:%d/%s/%s", serverPort, MetaServerKeeperService.PATH_PREFIX, MetaServerKeeperService.PATH_PING);
			template.postForObject(address, entity, String.class, clusterId, shardId);
			
			serverId++;
		}
		
	}

}
