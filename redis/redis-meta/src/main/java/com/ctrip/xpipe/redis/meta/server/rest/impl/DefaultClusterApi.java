package com.ctrip.xpipe.redis.meta.server.rest.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.rest.ClusterApi;

/**
 * @author wenchao.meng
 *
 * Jul 29, 2016
 */
@RestController
@RequestMapping("/api/metacluster/")
public class DefaultClusterApi implements ClusterApi{
	
	@Autowired
	private CurrentClusterServer currentClusterServer;

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

	@RequestMapping(path = "/notifyslotchange", method = RequestMethod.POST, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public void notifySlotChange() {
		currentClusterServer.notifySlotChange();
	}

	@RequestMapping(path = "/exportslot/{slotId}", method = RequestMethod.POST, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public void exportSlot(@PathVariable int slotId) throws Exception{
		currentClusterServer.exportSlot(slotId).sync();
	}

	@RequestMapping(path = "/importslot/{slotId}", method = RequestMethod.POST, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	@Override
	public void importSlot(@PathVariable int slotId) throws Exception{
		currentClusterServer.importSlot(slotId).sync();
	}
}
