package com.ctrip.xpipe.redis.console.constant;

/**
 * @author shyin
 *
 * Aug 26, 2016
 */
public final class XPipeConsoleConstant {
	public static int DEFAULT_DC_CLUSTER_PHASE = 1;
	public static long NO_ACTIVE_AZ_TAG = 0L;
	
	public static int MASTER_REQUIRED_TAG = -1;
	public static long NO_EXIST_ID = 0L;
	
	public static int MAX_NAME_SIZE = 128;
	
	public static String DEFAULT_ADDRESS = "0.0.0.0:0";
	public static String DEFAULT_IP = "0.0.0.0";
	public static int DEFAULT_PORT = 0;
	
	public static String ROLE_REDIS = "redis";
	public static String ROLE_KEEPER = "keeper";

	public static final String DEFAULT_XPIPE_USER = "xpipe";

	public static final long DEFAULT_ORG_ID = 0L;

	public static final int ONE_MINUTE = 60 * 1000;
	public static final int SCHEDULED_ORGANIZATION_SERVICE = 10 * ONE_MINUTE;

	public static final long SOCKET_TIMEOUT = 1L;
	
	private XPipeConsoleConstant(){
		
	}
}
