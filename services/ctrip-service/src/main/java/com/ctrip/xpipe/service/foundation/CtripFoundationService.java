package com.ctrip.xpipe.service.foundation;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.foundation.FoundationService;

/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class CtripFoundationService implements FoundationService{
	
	private static final String DATA_CENTER_KEY = "XPIPE_DATA_CENTER";

	@Override
	public String getDataCenter() {
		
		return System.getProperty(DATA_CENTER_KEY, Foundation.server().getDataCenter());
	}

	@Override
	public String getAppId() {
		return Foundation.app().getAppId();
	}

	@Override
	public String getLocalIp() {
		return Foundation.net().getHostAddress();
	}

	public String getEnvironment() {
		return Foundation.server().getEnv().getName();
	}

	@Override
	public int getOrder() {
		return HIGHEST_PRECEDENCE;
	}

}
