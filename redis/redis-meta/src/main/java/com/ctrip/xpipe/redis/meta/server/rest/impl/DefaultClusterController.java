package com.ctrip.xpipe.redis.meta.server.rest.impl;


import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.rest.ClusterApi;
import com.ctrip.xpipe.redis.meta.server.rest.ClusterDebugInfo;
import com.ctrip.xpipe.zk.ZkClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author wenchao.meng
 *
 * Jul 29, 2016
 */
@RestController
@RequestMapping(ClusterApi.PATH_PREFIX)
public class DefaultClusterController implements ClusterApi{
	
	@Autowired
	private CurrentClusterServer currentClusterServer;
	
	@Autowired
	private MetaServerConfig metaServerConfig;
	
	@Autowired
	private DcMetaCache dcMetaCache;
	
	@Autowired
	private SlotManager slotManager;

	@Autowired
	private ClusterServers<?> clusterServers;
	
	@Autowired
	private CurrentMetaManager  currentMetaServerMetaManager;
	
	@Autowired
	private ZkClient zkClient;

	@RequestMapping(path = "/serverid", method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public int getServerId() {
		return currentClusterServer.getServerId();
	}

	@RequestMapping(path = "/clusterinfo", method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public ClusterServerInfo getClusterInfo() {
		return currentClusterServer.getClusterInfo();
	}
	

	@RequestMapping(path = PATH_ADD_SLOT, method = RequestMethod.POST, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public void addSlot(@PathVariable int slotId) throws Exception {
		currentClusterServer.addSlot(slotId).sync();
	}

	@RequestMapping(path = PATH_DELETE_SLOT, method = RequestMethod.POST, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public void deleteSlot(@PathVariable int slotId) throws Exception {
		currentClusterServer.deleteSlot(slotId).sync();
	}

	@RequestMapping(path = PATH_EXPORT_SLOT, method = RequestMethod.POST, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public void exportSlot(@PathVariable int slotId) throws Exception{
		currentClusterServer.exportSlot(slotId).sync();
	}

	@RequestMapping(path = PATH_IMPORT_SLOT, method = RequestMethod.POST, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public void importSlot(@PathVariable int slotId) throws Exception{
		currentClusterServer.importSlot(slotId).sync();
	}
	
	
	@RequestMapping(path = PATH_NOTIFY_SLOT_CHANGE, method = RequestMethod.POST, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public void notifySlotChange(@PathVariable int slotId) throws Exception {
		currentClusterServer.notifySlotChange(slotId);
	}
	
	@RequestMapping(path = "/debug", method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public String debug() {
		JsonCodec pretty = new JsonCodec(true);
		return pretty.encode(
				new ClusterDebugInfo(currentClusterServer.getServerId(),
						dcMetaCache.getCurrentDc(),
						zkClient.getZkAddress(),
						metaServerConfig.getZkNameSpace(),
						currentClusterServer.isLeader(), 
						currentClusterServer.getClusterInfo(), 
						currentClusterServer.slots(),
						currentMetaServerMetaManager.allClusters(),
						clusterServers.allClusterServerInfos(),
						slotManager.allSlotsInfo()
						));
	}

	@RequestMapping(path = "/refresh", method = RequestMethod.POST, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public void refresh() throws Exception {
		slotManager.refresh();
		clusterServers.refresh();
	}
}
