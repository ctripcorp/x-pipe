package com.ctrip.xpipe.redis.core.route;

import com.ctrip.xpipe.utils.StringUtil;

public interface RouteChooseStrategyFactory {

    public enum RouteStrategyType {
        CRC32_HASH ;

        public static RouteStrategyType lookup(String name) {
            if (StringUtil.isEmpty(name)) throw new IllegalArgumentException("no RouteStrategyType for name " + name);
            return valueOf(name.toUpperCase());
        }
    }

    RouteChooseStrategy create(RouteStrategyType routeStrategyType);
}
