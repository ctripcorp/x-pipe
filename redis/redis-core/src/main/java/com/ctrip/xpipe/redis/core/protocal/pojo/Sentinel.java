package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.Objects;

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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Sentinel that = (Sentinel) o;

		return Objects.equals(name, that.name)
				&& Objects.equals(ip, that.ip)
				&& port == that.port;
	}

	@Override
	public int hashCode() {
		return ObjectUtils.hashCode(name, ip, port);
	}
}
