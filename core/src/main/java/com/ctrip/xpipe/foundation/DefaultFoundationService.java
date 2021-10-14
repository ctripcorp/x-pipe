/**
 * 
 */
package com.ctrip.xpipe.foundation;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author marsqing
 *
 *         Jun 15, 2016 7:27:34 PM
 */
public class DefaultFoundationService implements FoundationService {

	private static Logger logger = LoggerFactory.getLogger(DefaultFoundationService.class);

	public static final String DATA_CENTER_KEY = "datacenter";

	private Config config = Config.DEFAULT;

	private static AtomicBoolean logged = new AtomicBoolean(false);

	private static String dataCenter = null;
	
	private String appId = System.getProperty("appId", "appid_xpipe");

	private String localIp = System.getProperty("docker.proxy.localIp");

	public static void setDataCenter(String dataCenter) {
		DefaultFoundationService.dataCenter = dataCenter;
	}

	public DefaultFoundationService() {
		if (logged.compareAndSet(false, true)) {
			logger.info("data center is {}", dataCenter);
		}
	}

	@Override
	public String getDataCenter() {

		if(!StringUtil.isEmpty(dataCenter)){
			return dataCenter;
		}
		return config.get(DATA_CENTER_KEY, "jq");
	}

	@Override
	public String getAppId() {
		return appId;
	}
	
	public void setAppId(String appId) {
		this.appId = appId;
	}

	@Override
	public String getLocalIp() {
		try {
			if(localIp != null) return localIp;
			return Inet4Address.getLoopbackAddress().getHostAddress();
		} catch (Exception e) {
			return "127.0.0.1";
		}
	}

	@Override
	public String getGroupId() {
		return "group1";
	}

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}
}
