package com.ctrip.xpipe.redis.proxy.config;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import com.ctrip.xpipe.config.CompositeConfig;
import com.ctrip.xpipe.config.DefaultFileConfig;
import com.ctrip.xpipe.redis.proxy.handler.ZstdDecoder;
import com.ctrip.xpipe.redis.proxy.handler.ZstdEncoder;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */

@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultProxyConfig implements ProxyConfig {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyConfig.class);

    private Config config;

    private static final String PROXY_PROPERTIES_PATH = String.format("/opt/data/%s", FoundationService.DEFAULT.getAppId());

    private static final String PROXY_PROPERTIES_FILE = "xpipe.properties";

    private static final String KEY_ENDPOINT_HEALTH_CHECK_INTERVAL = "proxy.endpoint.check.interval.sec";

    private static final String KEY_PROXY_SOCKET_STATS_CHECK_INTERVAL = "proxy.socket.stats.check.interval.milli";

    private static final String KEY_TRAFFIC_REPORT_INTERVAL = "proxy.traffic.report.interval.milli";

    private static final String KEY_FRONTEND_TCP_PORT = "proxy.frontend.tcp.port";

    private static final String KEY_FRONTEND_TLS_PORT = "proxy.frontend.tls.port";

    private static final String KEY_NO_TLS_NETTY_HANDLER = "proxy.no.tls.netty.handler";

    private static final String KEY_INTERNAL_NETWORK_PREFIX = "proxy.internal.network.prefix";

    private static final String KEY_RECV_BUFFER_SIZE = "proxy.recv.buffer.size";

    private static final String KEY_START_PROXY_MONITOR = "proxy.monitor.start";

    private static final String KEY_PROXY_RESPONSE_TIMEOUT = "proxy.response.timeout";

    private static final String KEY_PROXY_COMPRESS_ENABLED = "proxy.compress.enabled";

    private static final String KEY_PROXY_COMPRESS_ALGORITHM = "proxy.compress.algorithm";

    private static final String KEY_PROXY_COMPRESS_ALGORITHM_VERSION = "proxy.compress.algorithm.version";

    private static final String KEY_PROXY_REPORT_TRAFFIC = "proxy.report.traffic";

    private static final String KEY_PROXY_BLOCK_WAIT_BASE_MILLi = "proxy.block.wait.base.milli";

    private static final String KEY_PROXY_BLOCK_RATE = "proxy.block.rate";

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("DefaultProxyConfig"));

    public DefaultProxyConfig() {
        config = initConfig();
        scheduledFresh();
    }

    public void scheduledFresh() {
        scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                config = initConfig();
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    private Config initConfig() {
        CompositeConfig compositeConfig = new CompositeConfig();
        try {
            compositeConfig.addConfig(new DefaultFileConfig(PROXY_PROPERTIES_PATH, PROXY_PROPERTIES_FILE));
        } catch (Exception e) {
            logger.warn("", e);
        }

        try {
            compositeConfig.addConfig(new DefaultFileConfig());
        } catch (Exception e) {
            logger.info("[DefaultProxyConfig]{}", e);
        }
        return compositeConfig;
    }

    @Override
    public int frontendTcpPort() {
        return getIntProperty(KEY_FRONTEND_TCP_PORT, 80);
    }

    @Override
    public int frontendTlsPort() {
        return getIntProperty(KEY_FRONTEND_TLS_PORT, 443);
    }

    @Override
    public long getTrafficReportIntervalMillis() {
        return getLongProperty(KEY_TRAFFIC_REPORT_INTERVAL, 30000L);
    }

    @Override
    public int endpointHealthCheckIntervalSec() {
        return getIntProperty(KEY_ENDPOINT_HEALTH_CHECK_INTERVAL, 60);
    }

    @Override
    public int socketStatsCheckInterval() {
        return getIntProperty(KEY_PROXY_SOCKET_STATS_CHECK_INTERVAL, 1000);
    }

    @Override
    public boolean noTlsNettyHandler() {
        return getBooleanProperty(KEY_NO_TLS_NETTY_HANDLER, false);
    }

    @Override
    public int getFixedRecvBufferSize() {
        return getIntProperty(KEY_RECV_BUFFER_SIZE, 4096);
    }

    @Override
    public String[] getInternalNetworkPrefix() {
        String internalNetworkPrefix = getProperty(KEY_INTERNAL_NETWORK_PREFIX, "10");
        return IpUtils.splitIpAddr(internalNetworkPrefix);
    }

    @Override
    public boolean startMonitor() {
        return getBooleanProperty(KEY_START_PROXY_MONITOR, false);
    }

    @Override
    public int getResponseTimeout() {
        return getIntProperty(KEY_PROXY_RESPONSE_TIMEOUT, 1000);
    }

    @Override
    public boolean isCompressEnabled() {
        return getBooleanProperty(KEY_PROXY_COMPRESS_ENABLED, Boolean.TRUE);
    }

    @Override
    public CompressAlgorithm getCompressAlgorithm() {
        return new CompressAlgorithm() {
            @Override
            public String version() {
                return getProperty(KEY_PROXY_COMPRESS_ALGORITHM_VERSION, "1.0");
            }

            @Override
            public AlgorithmType getType() {
                return AlgorithmType.valueOf(getProperty(KEY_PROXY_COMPRESS_ALGORITHM, AlgorithmType.ZSTD.name()));
            }
        };
    }

    @Override
    public boolean shouldReportTraffic() {
        return getBooleanProperty(KEY_PROXY_REPORT_TRAFFIC, false);
    }

    @Override
    public ByteToMessageDecoder getCompressDecoder() {
        return new ZstdDecoder();
    }

    @Override
    public MessageToByteEncoder<ByteBuf> getCompressEncoder() {
        return new ZstdEncoder();
    }

    @Override
    public String getServerCertChainFilePath() {
        return getProperty(KEY_SERVER_CERT_CHAIN_FILE_PATH, "/opt/data/100013684/openssl/server.crt");
    }

    @Override
    public String getClientCertChainFilePath() {
        return getProperty(KEY_CLIENT_CERT_CHAIN_FILE_PATH, "/opt/data/100013684/openssl/client.crt");
    }

    @Override
    public String getServerKeyFilePath() {
        return getProperty(KEY_SERVER_KEY_FILE_PATH, "/opt/data/100013684/openssl/pkcs8_server.crt");
    }

    @Override
    public String getClientKeyFilePath() {
        return getProperty(KEY_CLIENT_KEY_FILE_PATH, "/opt/data/100013684/openssl/pkcs8_client.key");
    }

    @Override
    public String getRootFilePath() {
        return getProperty(KEY_ROOT_FILE_PATH, "/opt/data/100013684/openssl/ca.crt");
    }

    @Override
    public int getBlockWaitBaseMill() {
        return getIntProperty(KEY_PROXY_BLOCK_WAIT_BASE_MILLi, 20000);
    }

    @Override
    public int getBlockWaitRate() {
        return getIntProperty(KEY_PROXY_BLOCK_RATE, 1000000);
    }

    protected String getProperty(String key, String defaultValue){
        return config.get(key, defaultValue);
    }

    protected Integer getIntProperty(String key, Integer defaultValue){

        String value = config.get(key);
        if(value == null){
            return defaultValue;
        }
        return Integer.parseInt(value.trim());

    }

    protected Long getLongProperty(String key, Long defaultValue){

        String value = config.get(key);
        if(value == null){
            return defaultValue;
        }
        //TODO cache value to avoid convert each time
        return Long.parseLong(value.trim());

    }

    protected Boolean getBooleanProperty(String key, Boolean defaultValue){

        String value = config.get(key);
        if(value == null){
            return defaultValue;
        }

        return Boolean.parseBoolean(value.trim());
    }
}
