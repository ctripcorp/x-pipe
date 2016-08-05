package com.ctrip.xpipe.redis.meta.server.config;


import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.config.DefaultFileConfig;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.utils.IpUtils;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 11:50:44 AM
 */
@Component
public class DefaultMetaServerConfig extends AbstractCoreConfig implements MetaServerConfig {
	
	public static String KEY_CONSOLE_ADDRESS = "console.adress";
	public static String KEY_META_REFRESH_MILLI = "meta.refresh.milli";
	public static String KEY_SLOT_REFRESH_MILLI = "slot.refresh.milli";
	public static String KEY_LEADER_CHACK_MILLI = "leader.check.milli";
	public static String KEY_CLUSTER_SERVERS_CHACK_MILLI = "cluster.servers.check.milli";
	
	
	public static String META_SRRVER_PROPERTIES_FILE = "meta_server.properties";
	public static String KEY_SERVER_ID = "metaserver.id";
	public static String KEY_SERVER_IP = "server.ip";
	public static String KEY_SERVER_PORT = "server.port";
	
	private String defaultConsoleAddress = System.getProperty("consoleAddress", "http://localhost:8080");
	private int defaultMetaServerId = Integer.parseInt(System.getProperty(KEY_SERVER_ID, "1"));
	private int defaultServerPort = Integer.parseInt(System.getProperty(KEY_SERVER_ID, "8080"));
	
	private Config serverConfig = new DefaultFileConfig(META_SRRVER_PROPERTIES_FILE); 

	@Override
	public String getConsoleAddress() {
		return getProperty(KEY_CONSOLE_ADDRESS, defaultConsoleAddress);
	}
	
	public void setDefaultConsoleAddress(String defaultConsoleAddress) {
		this.defaultConsoleAddress = defaultConsoleAddress;
	}

	public void setDefaultMetaServerId(int defaultMetaServerId) {
		this.defaultMetaServerId = defaultMetaServerId;
	}
	
	@Override
	public int getMetaRefreshMilli() {
		return getIntProperty(KEY_META_REFRESH_MILLI, 60000);
	}

	@Override
	public int getSlotRefreshMilli() {
		return getIntProperty(KEY_SLOT_REFRESH_MILLI, 60000);
	}

	@Override
	public int getLeaderCheckMilli() {
		return getIntProperty(KEY_LEADER_CHACK_MILLI, 60000);
	}

	@Override
	public int getClusterServersRefreshMilli() {
		return getIntProperty(KEY_CLUSTER_SERVERS_CHACK_MILLI, 60000);
	}



	//from local config file
	@Override
	public int getMetaServerId() {
		return Integer.parseInt(serverConfig.get(KEY_SERVER_ID, String.valueOf(defaultMetaServerId)));
	}

	@Override
	public String getMetaServerIp() {
		return serverConfig.get(KEY_SERVER_IP, IpUtils.getFistNonLocalIpv4ServerAddress().getHostAddress());
	}

	@Override
	public int getMetaServerPort() {
		return Integer.parseInt(serverConfig.get(KEY_SERVER_PORT, String.valueOf(defaultServerPort)));
	}
	
	public void setDefaultServerPort(int defaultServerPort) {
		this.defaultServerPort = defaultServerPort;
	}

}
