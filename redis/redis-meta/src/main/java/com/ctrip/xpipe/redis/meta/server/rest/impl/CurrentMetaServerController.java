package com.ctrip.xpipe.redis.meta.server.rest.impl;

import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.spring.AbstractController;

/**
 * @author wenchao.meng
 *
 * Aug 31, 2016
 */
@RestController
@RequestMapping("/api/current")
public class CurrentMetaServerController extends AbstractController{
	
	@Autowired
	private SlotManager slotManager;
	
	@Autowired
	private MetaServer 	currentMetaServer;

	
	@RequestMapping(path = "/slots", method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public Set<Integer> getSlots(){
		
		return slotManager.getSlotsByServerId(currentMetaServer.getServerId());
	}

	@RequestMapping(path = "/debug", method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public String getCurrentMeta(){
		
		String currentMeta = currentMetaServer.getCurrentMeta().toString(); 
		return currentMeta;
	}


}
