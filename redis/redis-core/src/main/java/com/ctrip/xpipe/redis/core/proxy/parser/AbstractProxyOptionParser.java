package com.ctrip.xpipe.redis.core.proxy.parser;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public abstract class AbstractProxyOptionParser implements ProxyOptionParser {

    public static final String LINE_SPLITTER = "\\s*;\\s*";
    public static final String ELEMENT_SPLITTER = "\\h";
    public static final String WHITE_SPACE = " ";
    public static final String ARRAY_SPLITTER = "\\s*,\\s*";

    protected String originOptionString;

    protected String output;

    @Override
    public ProxyOptionParser read(String option) {
        originOptionString = option;
        output = option;
        return this;
    }

    @Override
    public boolean isImportant() {
        return false;
    }
}
