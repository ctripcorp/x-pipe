package com.ctrip.xpipe.rest;

import java.util.concurrent.Callable;

import javax.ws.rs.core.Response;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public interface ProcessTemplate {
	
	Response process(Callable<Response>  callable);

}
