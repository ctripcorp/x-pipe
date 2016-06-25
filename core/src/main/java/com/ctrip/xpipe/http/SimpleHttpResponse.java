package com.ctrip.xpipe.http;

import org.apache.http.HttpResponse;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class SimpleHttpResponse {
	
	private HttpResponse httpResponse;
	
	public SimpleHttpResponse(HttpResponse httpResponse){
		this.httpResponse = httpResponse;
	}
	
	public int getStatusCode(){
		return httpResponse.getStatusLine().getStatusCode();
	}
	
	public boolean isSuccess(){
		
		return httpResponse.getStatusLine().getStatusCode() == HttpClientUtil.HTTP_STATUS_CODE_200; 
	}
	

}
