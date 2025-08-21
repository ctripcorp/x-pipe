package com.ctrip.xpipe.http;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;

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
	
	public String getResultAsString(String address) throws IOException, ParseException {
		
		HttpGet get = new HttpGet(address);
		CloseableHttpResponse response = null;
		try{
			response = httpClient.execute(get);
			if(response.getCode() != HTTP_STATUS_CODE_200){
				EntityUtils.consume(response.getEntity());
				throw new IllegalStateException("response code not 200");
			}
			HttpEntity entity = response.getEntity();
			return EntityUtils.toString(entity);
		} finally{
			if(response != null){
				response.close();
			}
		}
	}
}
