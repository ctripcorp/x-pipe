package com.ctrip.xpipe.endpoint;

import com.alibaba.fastjson.annotation.JSONField;
import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

@SuppressWarnings("serial")
public class DefaultEndPoint implements Endpoint, Serializable{
	
	private String rawUrl;
	private URI    uri;
	private String ip;
	private int port = 0;
	
	public DefaultEndPoint() {
	}

	public DefaultEndPoint(InetSocketAddress address) {
		this(address.getHostString(), address.getPort());
	}

	public DefaultEndPoint(String ip, int port){
		this("redis://" + ip + ":" + port);
		this.ip = ip;
		this.port = port;
	}
	public DefaultEndPoint(String url) {
		
		try {
			this.rawUrl = url;
			this.uri = new URI(url);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(url, e);
		}
	}

	@Override
	@JSONField(serialize=false)
	public String getScheme() {
		return uri.getScheme();
	}

	@Override
	@JSONField(serialize=false)
	public String getHost() {
		return ip != null ? ip : uri.getHost();
	}

	@Override
	@JSONField(serialize=false)
	public int getPort() {
		return port != 0 ? port : uri.getPort();
	}

	@Override
	@JSONField(serialize=false)
	public String getUser() {

		String []userInfo = getUserInfo();
		if(userInfo == null || userInfo.length != 2){
			return null;
		}
		return userInfo[0];
	}

	private String[] getUserInfo() {
		
		String userInfo = uri.getUserInfo();
		if(userInfo == null){
			return null;
		}
		String []split = userInfo.split("\\s*:\\s*");
		return split;
	}

	@Override
	@JSONField(serialize=false)
	public String getPassword() {
		
		String []userInfo = getUserInfo();
		if(userInfo == null || userInfo.length != 2){
			return null;
		}
		return userInfo[1];
	}
	
	@Override
	public String toString() {
		
		return rawUrl;
	}
	
	public String getRawUrl() {
		return rawUrl;
	}
	
	public void setRawUrl(String url) {
		try {
			this.rawUrl = url;
			this.uri = new URI(url);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(url, e);
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((uri == null) ? 0 : uri.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DefaultEndPoint other = (DefaultEndPoint) obj;
		if (uri == null) {
			if (other.uri != null)
				return false;
		} else if (!uri.equals(other.uri))
			return false;
		return true;
	}

	@Override
	public InetSocketAddress getSocketAddress() {
		return new InetSocketAddress(getHost(), getPort());
	}

}
