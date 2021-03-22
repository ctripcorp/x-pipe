package com.ctrip.xpipe.redis.core.console;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public class ConsoleCheckerPath {

    private ConsoleCheckerPath() {}

    public static final String PATH_GET_META = "/api/meta/divide/{index}";

    public static final String PATH_GET_PROXY_CHAINS = "/api/proxy/chains";

    public static final String PATH_PUT_CHECKER_STATUS = "/api/health/checker/status";

    public static final String PATH_PUT_HEALTH_CHECK_RESULT = "/api/health/check/result";

}
