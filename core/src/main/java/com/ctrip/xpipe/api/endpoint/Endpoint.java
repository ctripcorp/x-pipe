package com.ctrip.xpipe.api.endpoint;

public interface Endpoint {
	
	String getScheme();
	
	String getHost();
	
	int getPort();
	
	String getUser();
	
	String getPassword();
	

}
