package com.ctrip.xpipe.redis.console.rest;



import com.ctrip.xpipe.rest.RestApplication;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class ConsoleRestApplication extends RestApplication{
	
	public ConsoleRestApplication() {
		super("com.ctrip.xpipe.redis.console.rest.resource");
	}
}
