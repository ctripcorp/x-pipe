package com.ctrip.xpipe.redis.integratedtest.metaserver.proxy;

import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.handler.ZstdDecoder;
import com.ctrip.xpipe.redis.proxy.handler.ZstdEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LocalProxyConfig implements ProxyConfig {
    private static final Logger logger = LoggerFactory.getLogger(LocalProxyConfig.class);
    private int frontendTcpPort = 8992, frontendTlsPort = 1443;
    public static final String TCP_PORT = "tcp_port";
    public static final String TLS_PORT = "tls_port";
    public static final String CRT_DIR = "cert_dir";
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

    //    @Override
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

    //    @Override
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
    public ByteToMessageDecoder getCompressDecoder() {
        return new ZstdDecoder();
    }

    @Override
    public MessageToByteEncoder<ByteBuf> getCompressEncoder() {
        return new ZstdEncoder();
    }
    String certDir = "./src/test/resources/cert/";
    //    @Override
    public String getServerCertChainFilePath() {
        return certDir + "/server.crt";
    }

    //    @Override
    public String getClientCertChainFilePath() {
        return certDir + "/client.crt";
    }

    //    @Override
    public String getServerKeyFilePath() {
        return certDir + "/pkcs8_server.key";
    }

    //    @Override
    public String getClientKeyFilePath() {
        return certDir + "/pkcs8_client.key";
    }

    //    @Override
    public String getRootFilePath() {
        return certDir + "/ca.crt";
    }

    public LocalProxyConfig setFrontendTcpPort(int frontendTcpPort) {
        this.frontendTcpPort = frontendTcpPort;
        return this;
    }

    public LocalProxyConfig setFrontendTlsPort(int frontendTlsPort) {
        this.frontendTlsPort = frontendTlsPort;
        return this;
    }

    public LocalProxyConfig setStartMonitor(boolean startMonitor) {
        this.startMonitor = startMonitor;
        return this;
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    int getIntValueFromProperties(Properties p, String key, int def) {
        try {
            String str = p.getProperty(key);
            if(str == null) {
                return def;
            }
            return Integer.valueOf(str);
        } catch (Exception e) {
            return def;
        }
    }

    String getStringValueFromProperties(Properties p, String key, String def) {
        try {
            String str = p.getProperty(key);
            if(str == null) {
                return def;
            }
            return str;
        } catch (Exception e) {
            return def;
        }
    }

    public void loadFile(String file_name) throws IOException {
        logger.info("[proxy-config] load config file: {}", file_name);
        InputStream in = new BufferedInputStream(new FileInputStream(file_name));
        Properties p = new Properties();
        p.load(in);
        this.frontendTcpPort = getIntValueFromProperties(p, TCP_PORT, this.frontendTcpPort);
        this.frontendTlsPort = getIntValueFromProperties(p, TLS_PORT, this.frontendTlsPort);
        this.certDir = getStringValueFromProperties(p, CRT_DIR, ".");
        in.close();
    }

    public void parseArgs(String[] args) {
        for(int i = 0, len = args.length; i < len; i++) {
            switch(args[i].substring(2)) {
                case TCP_PORT:
                    this.setFrontendTcpPort(Integer.valueOf(args[++i])) ;
                    logger.info("[proxy-config] load args: tcp_port={}", this.frontendTcpPort);
                    break;
                case TLS_PORT:
                    this.setFrontendTlsPort(Integer.valueOf(args[++i])) ;
                    logger.info("[proxy-config] load args: tls_port={}", this.frontendTlsPort);
                    break;
                case CRT_DIR:
                    this.setCertDir(args[++i]);
                    logger.info("[proxy-config] load args: crt_dir={}", this.certDir);
                    break;
            }
        }
    }
    public void setCertDir(String path) {
        this.certDir = path;
    }
}
