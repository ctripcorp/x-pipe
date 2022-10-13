package com.ctrip.xpipe.redis.meta.server.config;


import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.CompositeConfig;
import com.ctrip.xpipe.config.DefaultFileConfig;
import com.ctrip.xpipe.config.DefaultPropertyConfig;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

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
	private static final String KEY_APPLIER_INFO_CHECK_INTERVAL = "meta.applier.info.check.interval";
	private static final String KEY_WAIT_FOR_META_SYNC_MILLI = "meta.sync.delay.milli";

	private static final String KEY_OWN_CLUSTER_TYPES = "meta.cluster.types";
	private static final String KEY_NEW_MASTER_CACHE_TIMEOUT_MILLI = "meta.cache.newmaster.timeout.milli";

	private static final String KEY_CORRECT_PEER_MASTER_PERIODICALLY = "meta.cluster.peermaster.correct.periodically";

	private static final String KEY_ROUTE_CHOOSE_STRATEGY_TYPE = "route.choose.strategy.type";

	private String defaultConsoleAddress = System.getProperty("consoleAddress", "http://localhost:8080");

	private String defaultRouteChooseStrategyType = RouteChooseStrategyFactory.RouteStrategyType.CRC32_HASH.name();
	
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

	@Override
	public int getApplierInfoCheckInterval() {
		return getIntProperty(KEY_APPLIER_INFO_CHECK_INTERVAL, 30 * 1000);
	}

	@Override
	public int getWaitForMetaSyncDelayMilli() {
		return getIntProperty(KEY_WAIT_FOR_META_SYNC_MILLI, 0);
	}

	@Override
	public Set<String> getOwnClusterType() {
		String clusterTypes = getProperty(KEY_OWN_CLUSTER_TYPES, ClusterType.ONE_WAY.toString());
		String[] split = clusterTypes.split("\\s*(,|;)\\s*");
		return Arrays.stream(split).filter(sp -> !StringUtil.isEmpty(sp)).collect(Collectors.toSet());
	}

	@Override
	public boolean shouldCorrectPeerMasterPeriodically() {
		return getBooleanProperty(KEY_CORRECT_PEER_MASTER_PERIODICALLY, true);
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

	@Override
	public long getNewMasterCacheTimeoutMilli() {
		return getLongProperty(KEY_NEW_MASTER_CACHE_TIMEOUT_MILLI, 5000L);
	}

	@Override
	public String getChooseRouteStrategyType() {
		return getProperty(KEY_ROUTE_CHOOSE_STRATEGY_TYPE, defaultRouteChooseStrategyType);
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
