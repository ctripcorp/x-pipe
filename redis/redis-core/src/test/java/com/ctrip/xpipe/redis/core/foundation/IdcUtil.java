package com.ctrip.xpipe.redis.core.foundation;


import com.ctrip.xpipe.foundation.DefaultFoundationService;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 2:08:06 PM
 */
public class IdcUtil {

	public final static int JQ_ZK_PORT = 2181;
	public final static int JQ_METASERVER_PORT = 9747;

	public final static int OY_ZK_PORT = 2182;
	public final static int OY_METASERVER_PORT = 9748;

	public static void setToJQ() {
		DefaultFoundationService.setDataCenter("jq");
		System.setProperty("metaServerPort", JQ_METASERVER_PORT + "");
		System.setProperty("zkPort", JQ_ZK_PORT + "");
	}

	public static void setToOY() {
		DefaultFoundationService.setDataCenter("oy");
		System.setProperty("metaServerPort", OY_METASERVER_PORT + "");
		System.setProperty("zkPort", OY_ZK_PORT + "");
	}
}
