package com.ctrip.xpipe.http;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class HttpClientUtil {
	
	public static final int HTTP_STATUS_CODE_200 = 200;
	
	private int maxTotal = 100;
    private int maxPerRoute = 10;
	
	private CloseableHttpClient httpClient;
	
	public HttpClientUtil() {
		httpClient = getHttpClient();
	}
	
	
	private CloseableHttpClient getHttpClient(){
		
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(maxTotal);
		cm.setDefaultMaxPerRoute(maxPerRoute);

        CloseableHttpClient httpclient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();		
        return httpclient;
	}
	
	public String getResultAsString(String address) throws IOException{
		
		HttpGet get = new HttpGet(address);
		CloseableHttpResponse response = null;
		try{
			response = httpClient.execute(get);
			String result = EntityUtils.toString(response.getEntity()); 
			if(response.getStatusLine().getStatusCode() != HTTP_STATUS_CODE_200){
				throw new IllegalStateException("response code not 200");
			}
			return result; 
		}finally{
			if(response != null){
				response.close();
			}
		}
	}
}
