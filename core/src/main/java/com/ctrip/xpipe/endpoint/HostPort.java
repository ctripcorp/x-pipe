package com.ctrip.xpipe.endpoint;

import com.ctrip.xpipe.tuple.Pair;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;

public class HostPort implements Serializable {

	private String m_host;

	private int m_port;

	public HostPort() {
	}

	public HostPort(InetSocketAddress address){
		this.m_host = address.getHostString();
		this.m_port = address.getPort();
	}

	public HostPort(String host, int port) {
		m_host = host;
		m_port = port;
	}

	public String getHost() {
		return m_host;
	}

	public void setHost(String host) {
		m_host = host;
	}

	public int getPort() {
		return m_port;
	}

	public void setPort(int port) {
		m_port = port;
	}

	public static HostPort fromPair(Pair<String, Integer> pair){

		return new HostPort(pair.getKey(), pair.getValue());
	}

	public static HostPort fromString(String addr) {
		String[] hostPort = addr.split("\\s*:\\s*");
		return new HostPort(hostPort[0], Integer.parseInt(hostPort[1]));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		HostPort hostPort = (HostPort) o;
		return m_port == hostPort.m_port &&
				  Objects.equals(m_host, hostPort.m_host);
	}

	@Override
	public int hashCode() {
		return Objects.hash(m_host) + m_port;
	}

	@Override
	public String toString() {
		return m_host + ":" + m_port;
	}
}