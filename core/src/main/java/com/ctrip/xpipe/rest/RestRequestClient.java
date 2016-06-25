package com.ctrip.xpipe.rest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.jackson.JacksonFeature;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class RestRequestClient {
	

	public static <R> R get(String address, Class<R> clazz){

		Client client = getClient();	
		WebTarget target = client.target(address);
        return target.request(MediaType.APPLICATION_JSON).get(clazz);
		
	}

	
	public static <R, T>  R request(String address, T request, Class<R> clazz){
		
		Client client = getClient();	
		WebTarget target = client.target(address);
		Entity<T> entity = Entity.entity(request, MediaType.APPLICATION_JSON);
        return target.request(MediaType.APPLICATION_JSON).post(entity, clazz);
	}
	
	public static <T>  Response request(String address, T request){
		
		Client client = getClient();
		WebTarget target = client.target(address);
		Entity<T> entity = Entity.entity(request, MediaType.APPLICATION_JSON);
		Response response = target.request(MediaType.APPLICATION_JSON).post(entity);
        return response;
	}

	private static Client getClient() {
		
		return ClientBuilder.newBuilder()
		.register(ObjectMapperProvider.class)
		.register(JacksonFeature.class)
		.build();
	}
}
