package com.ctrip.xpipe.redis.console.proxy.impl;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractNormalKeyValueTunnelSocketStatsAnalyzer extends AbstractTunnelSocketStatsAnalyzer {

    private String key;

    private final int fixedSkipLen;

    public AbstractNormalKeyValueTunnelSocketStatsAnalyzer(String key) {
        this.key = key;
        this.fixedSkipLen = key.length() + 1;
    }

    @Override
    protected double analyze(List<String> socketStats) {
        int index = -1;
        for(final String str : socketStats) {
            if((index = str.indexOf(key)) < 0) {
                continue;
            }
            int start = index + fixedSkipLen;
            int end = str.indexOf(" ", start);
            return getValue(str.substring(start, end));
        }
        return 0;
    }

    protected double getValue(String value) {
        return parseDouble(value);
    }

    protected double parseDouble(String str) {
        Pattern floatPattern = Pattern.compile("^([+-]?\\d*\\.?\\d*)$");
        Pattern longPattern = Pattern.compile("-?\\d+");
        Matcher matcher = floatPattern.matcher(str);
        if(!matcher.find()) {
            matcher = longPattern.matcher(str);
            if(matcher.find()) {
                return Long.parseLong(matcher.group(0));
            } else {
                logger.warn("[parseLong] regex not match: {}", str);
                return -1;
            }
        }
        return Double.parseDouble(matcher.group(0));

    }
}
