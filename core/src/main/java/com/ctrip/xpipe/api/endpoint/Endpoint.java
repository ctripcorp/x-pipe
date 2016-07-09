package com.ctrip.xpipe.api.endpoint;

import java.net.InetSocketAddress;

public interface Endpoint {
	
	String getScheme();
	
	String getHost();
	
	int getPort();
	
	String getUser();
	
	String getPassword();
	
	InetSocketAddress getSocketAddress();
}
