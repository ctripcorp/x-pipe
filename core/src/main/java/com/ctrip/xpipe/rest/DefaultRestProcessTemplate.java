package com.ctrip.xpipe.rest;

import java.util.concurrent.Callable;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class DefaultRestProcessTemplate implements ProcessTemplate{
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public Response process(Callable<Response> callable) {
		
		try{
			return callable.call();
		}catch(RestException e){
			logger.error("[process]", e);
			return e.getResponse();
		}catch(Exception e){
			logger.error("[process]", e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
	}
}
