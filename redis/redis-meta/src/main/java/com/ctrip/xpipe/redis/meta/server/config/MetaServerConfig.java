package com.ctrip.xpipe.redis.meta.server.config;

import java.util.Map;

import com.ctrip.xpipe.redis.core.config.CoreConfig;
import com.ctrip.xpipe.redis.core.meta.DcInfo;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 11:48:44 AM
 */
public interface MetaServerConfig extends CoreConfig {

	String getConsoleAddress();

	int getMetaRefreshMilli();

	int getMetaServerId();

	String getMetaServerIp();

	int getMetaServerPort();

	int getClusterServersRefreshMilli();

	int getSlotRefreshMilli();

	int getLeaderCheckMilli();

	Map<String, DcInfo> getDcInofs();

	int getWaitforOffsetMilli();
}
