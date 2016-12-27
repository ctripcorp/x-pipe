package com.ctrip.xpipe.redis.core.protocal.pojo;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public class Sentinel {
	
	private String name;
	private String ip;
	private int port;
	

	public Sentinel(String name, String ip, int port){
		this.name = name;
		this.ip = ip;
		this.port = port;
	}

	public String getName() {
		return name;
	}


	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}
	
	@Override
	public String toString() {
		return String.format("name:%s, ip:%s, port:%d", name, ip, port);
	}
}
