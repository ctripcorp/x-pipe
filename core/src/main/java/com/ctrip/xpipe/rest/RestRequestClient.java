package com.ctrip.xpipe.rest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class RestRequestClient {
	
	
	private static final int DEFAULT_CONNECT_TIMEOUT = 5000;
	
	private static final int DEFAULT_READ_TIMEOUT = 5000;
	
	public static <R> R get(String address, Class<R> clazz){

		return get(address, clazz, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT);
	}

	
	public static <R, T>  R post(String address, T request, Class<R> clazz){

		Response response = post(address, request);
		
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
    		return response.readEntity(clazz);
        }
        throw new IllegalStateException("response fail:" + response);  
	}

	public static <T>  Response post(String address, T request){
		return post(address, request, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT);
	}

	public static <T>  Response post(String address, T request, int connectTimeout, int readTimeout){
		
		Client client = getClient();
		WebTarget target = client.target(address);
		Entity<T> entity = Entity.entity(request, MediaType.APPLICATION_JSON);
		Builder builder = target.request(MediaType.APPLICATION_JSON);
		builder.property(ClientProperties.CONNECT_TIMEOUT, connectTimeout);
		builder.property(ClientProperties.READ_TIMEOUT, readTimeout);
        return builder.post(entity);
	}

	public static <R> R get(String address, Class<R> clazz, int connectTimeout, int readTimeout){

		Client client = getClient();	
		WebTarget target = client.target(address);
		Builder builder = target.request(MediaType.APPLICATION_JSON);
		builder.property(ClientProperties.CONNECT_TIMEOUT, connectTimeout);
		builder.property(ClientProperties.READ_TIMEOUT, readTimeout);
        return builder.get(clazz);
		
	}

	private static Client getClient() {
		
		return ClientBuilder.newBuilder()
		.register(ObjectMapperProvider.class)
		.register(JacksonFeature.class)
		.build();
	}
}
