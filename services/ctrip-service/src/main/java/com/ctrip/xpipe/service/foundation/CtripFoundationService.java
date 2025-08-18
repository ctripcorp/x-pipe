package com.ctrip.xpipe.service.foundation;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.Map;

/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class CtripFoundationService implements FoundationService{
	
	protected static final String DATA_CENTER_KEY = "XPIPE_DATA_CENTER";

	protected static final String GROUP_ID_KEY = "XPIPE_GROUP_ID";

	private static FoundationConfig config = new FoundationConfig();

	@Override
	public String getDataCenter() {
		String dataCenter = null;
		Map<String, String> groupDcMap = config.getGroupDcMap();
		String groupId = System.getProperty(GROUP_ID_KEY, Foundation.group().getId());
		if (null != groupDcMap && groupDcMap.containsKey(groupId))  dataCenter = groupDcMap.get(groupId);
		else  dataCenter = System.getProperty(DATA_CENTER_KEY, Foundation.server().getDataCenter());
		if(dataCenter == null){
			dataCenter = "NODC_DEFINED";
		}
		return dataCenter;
	}

	@Override
	public String getAppId() {
		return Foundation.app().getAppId();
	}

	@Override
	public String getLocalIp() {
		return Foundation.net().getHostAddress();
	}

	public int getHttpPort() {
		return Foundation.web().getHttpPort();
	}

	@Override
	public String getGroupId() {
		return Foundation.group().getId();
	}

	public String getEnvironment() {
		return Foundation.server().getEnv().getName();
	}

	@Override
	public String getRegion() {
		return Foundation.server().getRegion();
	}

	@Override
	public int getOrder() {
		return HIGHEST_PRECEDENCE;
	}

	@VisibleForTesting
	protected static void setConfig(FoundationConfig config) {
		CtripFoundationService.config = config;
	}

}
