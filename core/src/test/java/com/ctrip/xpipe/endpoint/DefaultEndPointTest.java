package com.ctrip.xpipe.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;

public class DefaultEndPointTest {
	
	@Test
	public void testEndPoint(){
		
		String url="redis://:password@10.2.58.242:6379";
		
		Endpoint endpoint = new DefaultEndPoint(url);
		
		Assert.assertEquals("redis", endpoint.getScheme());
		Assert.assertEquals("", endpoint.getUser());
		Assert.assertEquals("password", endpoint.getPassword());
		Assert.assertEquals("10.2.58.242", endpoint.getHost());
		Assert.assertEquals(6379, endpoint.getPort());
	}

	@Test
	public void testProxyEndpoint() {
		String url = "proxy://10.1.1.1:6379";

		Endpoint endpoint = new DefaultEndPoint(url);

		Assert.assertEquals("proxy", endpoint.getScheme());
		Assert.assertEquals("10.1.1.1", endpoint.getHost());
		Assert.assertEquals(6379, endpoint.getPort());

		String uri = "proxytls://10.1.1.1:6379";

		endpoint = new DefaultEndPoint(uri);

		Assert.assertEquals("proxytls", endpoint.getScheme());
		Assert.assertEquals("10.1.1.1", endpoint.getHost());
		Assert.assertEquals(6379, endpoint.getPort());
		Assert.assertEquals(uri, ((DefaultEndPoint) endpoint).getRawUrl());

		SocketAddress address = endpoint.getSocketAddress();
		System.out.println(((InetSocketAddress) address).getHostName());
		System.out.println(((InetSocketAddress) address).getAddress());
		System.out.println(((InetSocketAddress) address).getHostString());
	}

	@Test
	public void testEndpointAsKeyOfMap() {

		HashMap<Endpoint, Object> map = new HashMap<>();
		Object item = new Object();

		ProxyConnectProtocol protocol = mock(ProxyConnectProtocol.class);

		Endpoint endpoint1 = new DefaultEndPoint("127.0.0.1", 6379);
		Endpoint endpoint2 = new DefaultEndPoint("127.0.0.1", 6379, protocol);
		Endpoint endpoint1_1 = new DefaultEndPoint("127.0.0.1", 6379);

		assertNotEquals(endpoint1, endpoint2);
		assertEquals(endpoint1.hashCode(), endpoint2.hashCode());

		map.put(endpoint1, item);

		assertEquals(item, map.get(endpoint1_1));
		assertNotEquals(item, map.get(endpoint2));
	}
}
