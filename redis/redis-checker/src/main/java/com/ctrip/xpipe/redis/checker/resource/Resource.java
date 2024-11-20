package com.ctrip.xpipe.redis.checker.resource;

/**
 * @author lishanglin
 * date 2021/3/8
 */
public class Resource {

    public static final String REDIS_COMMAND_EXECUTOR = "redisCommandExecutor";

    public static final String REDIS_KEYED_NETTY_CLIENT_POOL = "redisKeyedClientPool";

    public static final String SENTINEL_KEYED_NETTY_CLIENT_POOL = "sentinelKeyedClientPool";

    public static final String KEEPER_KEYED_NETTY_CLIENT_POOL = "keeperKeyedClientPool";

    public static final String PROXY_KEYED_NETTY_CLIENT_POOL = "proxyKeyedClientPool";

    public static final String REDIS_SESSION_NETTY_CLIENT_POOL = "redisSessionClientPool";

    public static final String PING_DELAY_INFO_EXECUTORS = "pingDelayInfoExecutors";

    public static final String PING_DELAY_INFO_SCHEDULED = "pingDelayInfoScheduled";

    public static final String HELLO_CHECK_EXECUTORS = "helloCheckExecutors";

    public static final String HELLO_CHECK_SCHEDULED = "helloCheckScheduled";

}
