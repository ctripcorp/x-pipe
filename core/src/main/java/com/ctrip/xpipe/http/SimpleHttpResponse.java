package com.ctrip.xpipe.http;

import org.apache.hc.core5.http.HttpResponse;

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
		return httpResponse.getCode();
	}
	
	public boolean isSuccess(){
		
		return httpResponse.getCode() == HttpClientUtil.HTTP_STATUS_CODE_200;
	}
	

}
