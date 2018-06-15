package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.utils.ObjectUtils;

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
}
