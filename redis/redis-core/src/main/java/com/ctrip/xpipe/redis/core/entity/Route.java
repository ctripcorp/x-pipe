package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author chen.zhu
 * <p>
 * Jun 05, 2018
 */
public interface Route {

    public static String TAG_META = "meta";
    public static String TAG_CONSOLE = "console";

    default boolean tagEquals(String tag){
        return ObjectUtils.equals(tag, getTag(), ((obj1, obj2) -> obj1.equalsIgnoreCase(obj2)));
    }

    String getRouteInfo();

    String getTag();

    default String routeProtocol(){

        String routeInfo = getRouteInfo();
        if(StringUtil.isEmpty(routeInfo)){
            return "";
        }
        return String.format("%s %s %s", ProxyProtocol.KEY_WORD, PROXY_OPTION.ROUTE.name(), routeInfo.trim());
    }
}
