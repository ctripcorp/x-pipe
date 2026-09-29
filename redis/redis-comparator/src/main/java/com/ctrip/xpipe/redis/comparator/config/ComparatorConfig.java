package com.ctrip.xpipe.redis.comparator.config;

import com.ctrip.xpipe.config.AbstractConfigBean;
import org.springframework.context.annotation.Configuration;

/**
 * comparator QConfig（spec §5）。读 {@code Config.DEFAULT} SPI，与 Checker {@code CheckConfigBean} 同模式。
 */
@Configuration
public class ComparatorConfig extends AbstractConfigBean {

    public static final String KEY_CONSOLE_ADDRESS = "console.address";
    public static final String KEY_META_REFRESH_INTERVAL_MILLI = "comparator.meta.refresh.interval.milli";
    public static final String KEY_STREAM_BUFFER_BYTES = "comparator.stream.buffer.bytes";
    public static final String KEY_COMPARE_CHUNK_BYTES = "comparator.compare.chunk.bytes";
    public static final String KEY_MISMATCH_DUMP_BYTES = "comparator.mismatch.dump.bytes";
    public static final String KEY_CMS_ACCESS_TOKEN = "comparator.cms.access.token";
    public static final String KEY_CMS_GET_SERVER_URL = "comparator.cms.get.server.url";

    public static final String DEFAULT_CONSOLE_ADDRESS = "";
    public static final int DEFAULT_META_REFRESH_INTERVAL_MILLI = 30000;
    public static final int DEFAULT_STREAM_BUFFER_BYTES = 2097152;
    public static final int DEFAULT_COMPARE_CHUNK_BYTES = 65536;
    public static final int DEFAULT_MISMATCH_DUMP_BYTES = 128;
    public static final String DEFAULT_CMS_ACCESS_TOKEN = "";
    public static final String DEFAULT_CMS_GET_SERVER_URL = "";

    public String getConsoleAddress() {
        return getProperty(KEY_CONSOLE_ADDRESS, DEFAULT_CONSOLE_ADDRESS);
    }

    public int getMetaRefreshIntervalMilli() {
        return getIntProperty(KEY_META_REFRESH_INTERVAL_MILLI, DEFAULT_META_REFRESH_INTERVAL_MILLI);
    }

    public int getStreamBufferBytes() {
        return getIntProperty(KEY_STREAM_BUFFER_BYTES, DEFAULT_STREAM_BUFFER_BYTES);
    }

    public int getCompareChunkBytes() {
        return getIntProperty(KEY_COMPARE_CHUNK_BYTES, DEFAULT_COMPARE_CHUNK_BYTES);
    }

    public int getMismatchDumpBytes() {
        return getIntProperty(KEY_MISMATCH_DUMP_BYTES, DEFAULT_MISMATCH_DUMP_BYTES);
    }

    public String getCmsAccessToken() {
        return getProperty(KEY_CMS_ACCESS_TOKEN, DEFAULT_CMS_ACCESS_TOKEN);
    }

    public String getCmsGetServerUrl() {
        return getProperty(KEY_CMS_GET_SERVER_URL, DEFAULT_CMS_GET_SERVER_URL);
    }
}
