package com.ctrip.xpipe.redis.core.metaserver;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.rest.ForwardType;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *
 * Nov 30, 2016
 */
public enum META_SERVER_SERVICE {
	

	//common
	GET_ACTIVE_KEEPER(PATH.GET_ACTIVE_KEEPER, ForwardType.FORWARD),
	
	//console
	CLUSTER_CHANGE(PATH.PATH_CLUSTER_CHANGE, ForwardType.MULTICASTING),
	CHANGE_PRIMARY_DC_CHECK(PATH.PATH_CHANGE_PRIMARY_DC_CHECK, ForwardType.FORWARD),
	CHANGE_PRIMARY_DC(PATH.PATH_CHANGE_PRIMARY_DC, ForwardType.FORWARD),
	MARK_MASTER_READONLY(PATH.PATH_MARK_MASTER_READONLY, ForwardType.FORWARD),
	
	//keeper
	
	//multi dc
	UPSTREAM_CHANGE(PATH.PATH_UPSTREAM_CHANGE, ForwardType.FORWARD)
	;	
	private String path;
	private ForwardType forwardType;

	META_SERVER_SERVICE(String path, ForwardType forwardType){
		this.path = path;
		this.forwardType = forwardType;
	}

	public String getPath() {
		return path;
	}
	
	public ForwardType getForwardType() {
		return forwardType;
	}
	
	public String getRealPath(String host){
		
		if(!host.startsWith("http")){
			host += "http://";
		}
		return String.format("%s/%s/%s", host, PATH.PATH_PREFIX, getPath());
	}
	
	
	protected boolean match(String realPath){
		
		String []realsp = StringUtil.splitRemoveEmpty("\\/+", realPath);
		String []sp = StringUtil.splitRemoveEmpty("\\/+", getPath());
		
		if(sp.length != realsp.length){
			return false;
		}
		for(int i=0;i<sp.length;i++){
			
			if(sp[i].startsWith("{")){
				continue;
			}
			if(!realsp[i].equalsIgnoreCase(sp[i])){
				return false;
			}
		}
		return true;
	}
	
	public static META_SERVER_SERVICE fromPath(String path){
		return fromPath(path, PATH.PATH_PREFIX);
	}
	
	protected static META_SERVER_SERVICE fromPath(String path, String prefix){

		List<META_SERVER_SERVICE> matched = new LinkedList<>();
		if(prefix != null && path.startsWith(prefix)){
			path =  path.substring(prefix.length());
		}
		
		for(META_SERVER_SERVICE service : META_SERVER_SERVICE.values()){
			if(service.match(path)){
				matched.add(service);
			}
		}
		if(matched.size() == 1){
			return matched.get(0);
		}
		throw new IllegalStateException("from path:" + path + ", we found matches:" + matched);
	}

	public static class PATH{
		
		public static final String PATH_PREFIX = "/api/meta";
		
		//common
		
		public static final String GET_ACTIVE_KEEPER = "/getactivekeeper/{clusterId}/{shardId}";
		
		//keeper
		
		//console
		public static final String PATH_CLUSTER_CHANGE = "/clusterchange/{clusterId}";
		public static final String PATH_CHANGE_PRIMARY_DC_CHECK = "/changeprimarydc/check/{clusterId}/{shardId}/{newPrimaryDc}";
		public static final String PATH_CHANGE_PRIMARY_DC = "/changeprimarydc/{clusterId}/{shardId}/{newPrimaryDc}";
		public static final String PATH_MARK_MASTER_READONLY = "/masterreadonly/{clusterId}/{shardId}";
		
		//multi dc
		public static final String PATH_UPSTREAM_CHANGE = "/upstreamchange/{clusterId}/{shardId}/{ip}/{port}";

	}
	
}
