package com.ctrip.xpipe.redis.core.proxy.parser;


import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public interface ProxyOptionParser {

    String output();

    ProxyOptionParser read(String option);

    PROXY_OPTION option();

    boolean isImportant();

}
