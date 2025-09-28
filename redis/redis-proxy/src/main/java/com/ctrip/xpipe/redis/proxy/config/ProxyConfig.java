package com.ctrip.xpipe.redis.proxy.config;


import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import com.ctrip.xpipe.redis.core.config.TLSConfig;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyConfig extends TLSConfig {

    int frontendTcpPort();

    int frontendTlsPort();

    long getTrafficReportIntervalMillis();

    int endpointHealthCheckIntervalSec();

    int socketStatsCheckInterval();

    boolean noTlsNettyHandler();

    int getFixedRecvBufferSize();

    // to avoid any connect outside internal network
    String[] getInternalNetworkPrefix();

    boolean startMonitor();

    int getResponseTimeout();

    boolean isCompressEnabled();

    CompressAlgorithm getCompressAlgorithm();

    ByteToMessageDecoder getCompressDecoder();

    MessageToByteEncoder<ByteBuf> getCompressEncoder();

    boolean shouldReportTraffic();

    int getBlockWaitBaseMill();

    int getBlockWaitRate();

    /**
     * Check if cross-region traffic control is enabled
     * @return true if enabled, false otherwise
     */
    boolean isCrossRegionTrafficControlEnabled();

    /**
     * Get the global throughput limit for cross-region traffic control in bytes per second
     * @return throughput limit in bytes per second
     */
    long getCrossRegionTrafficControlLimit();

    /**
     * DRC does not allow the proxy to actively close the channel during backpressure.
     * @return true if allowing the proxy to actively close the connection, false otherwise.
     */
    boolean allowCloseChannel();
}
