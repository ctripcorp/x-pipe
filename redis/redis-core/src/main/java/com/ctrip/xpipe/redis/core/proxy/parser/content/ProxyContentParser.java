package com.ctrip.xpipe.redis.core.proxy.parser.content;

public interface ProxyContentParser {

    SubOptionParser getSubOption();

    ContentType getContentType();

    interface SubOptionParser {
        String output();
        SubOptionParser parse(String... subOption);
    }

    enum ContentType {
        COMPRESS
    }
}
