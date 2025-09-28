package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.handler.ZstdDecoder;
import com.ctrip.xpipe.redis.proxy.handler.ZstdEncoder;
import com.ctrip.xpipe.redis.proxy.ssl.GenerateCertificates;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class TestProxyConfig implements ProxyConfig {

    private int frontendTcpPort = 8992, frontendTlsPort = 443;

    private boolean startMonitor = false;

    private boolean compress = false;

    @Override
    public int frontendTcpPort() {
        return frontendTcpPort;
    }

    @Override
    public int frontendTlsPort() {
        return frontendTlsPort;
    }

    @Override
    public long getTrafficReportIntervalMillis() {
        return 1000;
    }

    @Override
    public int endpointHealthCheckIntervalSec() {
        return 60;
    }

    @Override
    public int socketStatsCheckInterval() {
        return 1000;
    }

    @Override
    public boolean noTlsNettyHandler() {
        return false;
    }

    @Override
    public int getFixedRecvBufferSize() {
        return 1024;
    }

    @Override
    public String[] getInternalNetworkPrefix() {
        return null;
    }

    @Override
    public boolean startMonitor() {
        return startMonitor;
    }

    @Override
    public int getResponseTimeout() {
        return 100;
    }

    @Override
    public boolean isCompressEnabled() {
        return compress;
    }

    @Override
    public CompressAlgorithm getCompressAlgorithm() {
        return new CompressAlgorithm() {
            @Override
            public String version() {
                return "1.0";
            }

            @Override
            public AlgorithmType getType() {
                return AlgorithmType.ZSTD;
            }
        };
    }

    @Override
    public boolean shouldReportTraffic() {
        return true;
    }

    @Override
    public ByteToMessageDecoder getCompressDecoder() {
        return new ZstdDecoder();
    }

    @Override
    public MessageToByteEncoder<ByteBuf> getCompressEncoder() {
        return new ZstdEncoder();
    }

    String certDir = System.getProperty("java.io.tmpdir");

    static {
        try {
            GenerateCertificates.generateFile();
        } catch (Exception e) {
        }
    }

    static {
        try {
            GenerateCertificates.generateFile();
        } catch (Exception e) {
        }
    }

    @Override
    public int getBlockWaitBaseMill() {
        return 1000;
    }

    @Override
    public int getBlockWaitRate() {
        return 1000;
    }

    @Override
    public String getServerCertChainFilePath() {
        return certDir + "/server.crt";
    }

    @Override
    public String getClientCertChainFilePath() {
        return certDir + "/client.crt";
    }

    @Override
    public String getServerKeyFilePath() {
        return certDir + "/pkcs8_server.key";
    }

    @Override
    public String getClientKeyFilePath() {
        return certDir + "/pkcs8_client.key";
    }

    @Override
    public String getRootFilePath() {
        return certDir + "/ca.crt";
    }

    @Override
    public boolean isCrossRegionTrafficControlEnabled() {
        return false;
    }

    @Override
    public long getCrossRegionTrafficControlLimit() {
        return 104857600L; // 100MB/s default
    }

    public TestProxyConfig setFrontendTcpPort(int frontendTcpPort) {
        this.frontendTcpPort = frontendTcpPort;
        return this;
    }

    public TestProxyConfig setFrontendTlsPort(int frontendTlsPort) {
        this.frontendTlsPort = frontendTlsPort;
        return this;
    }

    public TestProxyConfig setStartMonitor(boolean startMonitor) {
        this.startMonitor = startMonitor;
        return this;
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    @Override
    public boolean allowCloseChannel() {
        return true;
    }
}
