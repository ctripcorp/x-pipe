package com.ctrip.xpipe.redis.meta.server.rest;

import com.ctrip.xpipe.rest.ForwardType;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class ForwardInfo implements Cloneable{
	
	private ForwardType type;
	
	private List<Integer> forwardServers = new LinkedList<>();

	public ForwardInfo(){
		
	}

	public ForwardInfo(ForwardType type){
		this(type, null);
	}

	public ForwardInfo(ForwardType type, Integer forwardServer){
		
		this.type = type;
		if(forwardServer != null){
			this.forwardServers.add(forwardServer);
		}
	}
	

	public ForwardType getType() {
		return type;
	}

	public void setType(ForwardType type) {
		this.type = type;
	}

	public List<Integer> getForwardServers() {
		return forwardServers;
	}
	
	public boolean hasServer(Integer serverId){
		
		for(Integer passServerId : forwardServers){
			if(passServerId.equals(serverId)){
				return true;
			}
		}
		return false;
	}

	public void setForwardServers(List<Integer> forwardServers) {
		this.forwardServers = forwardServers;
	}
	
	public void addForwardServers(Integer server){
		forwardServers.add(server);
	}
	
	@Override
	public String toString() {
		return String.format("type:%s, fromServers:%s", type, forwardServers);
	}
	
	@Override
	public ForwardInfo clone() {
		
		ForwardInfo forwardInfo;
		try {
			forwardInfo = (ForwardInfo) super.clone();
			forwardInfo.forwardServers = new LinkedList<>(forwardServers);
			return forwardInfo;
		} catch (CloneNotSupportedException e) {
		}
		return null;
	}
}
