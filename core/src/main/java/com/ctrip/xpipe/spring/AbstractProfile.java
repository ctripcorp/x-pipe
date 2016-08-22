package com.ctrip.xpipe.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Aug 22, 2016
 */
public abstract class AbstractProfile {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	public final static String PROFILE_KEY = "spring.profiles.active";
	
	public final static String PROFILE_NAME_PRODUCTION = "production";
	public final static String PROFILE_NAME_TEST = "test";
	

}
