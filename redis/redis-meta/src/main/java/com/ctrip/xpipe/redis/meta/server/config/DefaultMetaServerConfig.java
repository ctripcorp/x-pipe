package com.ctrip.xpipe.redis.meta.server.config;


import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.CompositeConfig;
import com.ctrip.xpipe.config.DefaultFileConfig;
import com.ctrip.xpipe.config.DefaultPropertyConfig;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.utils.IpUtils;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 11:50:44 AM
 */
public class DefaultMetaServerConfig extends AbstractCoreConfig implements MetaServerConfig {
	
	public static String KEY_CONSOLE_ADDRESS = "console.address";
	public static String KEY_META_REFRESH_MILLI = "meta.refresh.milli";
	public static String KEY_SLOT_REFRESH_MILLI = "slot.refresh.milli";
	public static String KEY_LEADER_CHACK_MILLI = "leader.check.milli";
	public static String KEY_CLUSTER_SERVERS_CHACK_MILLI = "cluster.servers.check.milli";
	public static String KEY_WAITFOR_OFFSET_MILLI = "dcchange.waitfor.offset.milli";
	public static String KEY_DC_INFOS = "dcinfos";
	public static String KEY_VALIDATE_DOMAIN = "metaserver.validate.domain";
	
	
	public static String META_SRRVER_PROPERTIES_PATH = String.format("/opt/data/%s", FoundationService.DEFAULT.getAppId());
	public static String META_SRRVER_PROPERTIES_FILE = "meta_server.properties";
	
	public static String KEY_SERVER_ID = "metaserver.id";
	public static String KEY_SERVER_IP = "server.ip";
	public static String KEY_SERVER_PORT = "server.port";

	private static final String KEY_KEEPER_INFO_CHECK_INTERVAL = "meta.keeper.info.check.interval";
	
	private String defaultConsoleAddress = System.getProperty("consoleAddress", "http://localhost:8080");
	
	private int defaultMetaServerId = Integer.parseInt(System.getProperty(KEY_SERVER_ID, "1"));
	private int defaultServerPort = Integer.parseInt(System.getProperty(KEY_SERVER_ID, "8080"));
	
	private Config serverConfig;

	private Map<String, DcInfo> dcInfos = Maps.newConcurrentMap();

	public DefaultMetaServerConfig(){

		CompositeConfig compositeConfig = new CompositeConfig();
		try{
			compositeConfig.addConfig(new DefaultFileConfig(META_SRRVER_PROPERTIES_PATH, META_SRRVER_PROPERTIES_FILE));
		}catch (Exception e){
			logger.info("[DefaultMetaServerConfig]{}", e);
		}
		try{
			compositeConfig.addConfig(new DefaultFileConfig());
		}catch (Exception e){
			logger.info("[DefaultMetaServerConfig]{}", e);
		}
		compositeConfig.addConfig(new DefaultPropertyConfig());
		serverConfig = compositeConfig;
	}

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

	@Override
	public Map<String, DcInfo> getDcInofs() {
		if(dcInfos.isEmpty()) {
			dcInfos = getDcInofMapping();
		}
		return dcInfos;
	}

	private Map<String, DcInfo> getDcInofMapping() {

		String dcInfoStr = getProperty(KEY_DC_INFOS, "{}");
		Map<String, DcInfo> dcInfos = JsonCodec.INSTANCE.decode(dcInfoStr, new GenericTypeReference<Map<String, DcInfo>>() {
		});

		Map<String, DcInfo> result = Maps.newConcurrentMap();
		for(Entry<String, DcInfo> entry : dcInfos.entrySet()){
			result.put(entry.getKey().toLowerCase(), entry.getValue());
		}

		logger.debug("[getDcInofs]{}", result);
		return result;
	}

	@Override
	public int getWaitforOffsetMilli() {
		return getIntProperty(KEY_WAITFOR_OFFSET_MILLI, 2000);
	}

	@Override
	public boolean validateDomain() {
		return getBooleanProperty(KEY_VALIDATE_DOMAIN, true);
	}

	@Override
	public int getKeeperInfoCheckInterval() {
		return getIntProperty(KEY_KEEPER_INFO_CHECK_INTERVAL, 30 * 1000);
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

	@Override
	public void onChange(String key, String oldValue, String newValue) {
		super.onChange(key, oldValue, newValue);
		dcInfos = getDcInofMapping();
	}

}
