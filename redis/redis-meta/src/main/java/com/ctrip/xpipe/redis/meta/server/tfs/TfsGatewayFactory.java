package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Creates {@link TfsGateway} from QConfig endpoint.
 */
public final class TfsGatewayFactory {

    public static final String MOCK_GATEWAY_PREFIX = "mock://";

    private static final Logger logger = LoggerFactory.getLogger(TfsGatewayFactory.class);

    private static final ConcurrentMap<String, TfsGateway> GATEWAY_CACHE = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, Boolean> UNIMPLEMENTED_ENDPOINT_LOGGED = new ConcurrentHashMap<>();

    private TfsGatewayFactory() {
    }

    public static TfsGateway create(String endpoint) {
        String cacheKey = StringUtil.isEmpty(endpoint) ? "" : endpoint;
        return GATEWAY_CACHE.computeIfAbsent(cacheKey, TfsGatewayFactory::doCreate);
    }

    private static TfsGateway doCreate(String endpoint) {
        if (StringUtil.isEmpty(endpoint) || endpoint.startsWith(MOCK_GATEWAY_PREFIX)) {
            return new MockTfsGateway();
        }
        if (UNIMPLEMENTED_ENDPOINT_LOGGED.putIfAbsent(endpoint, Boolean.TRUE) == null) {
            logger.error("[create][real TFS gateway is not implemented in M1, fallback to failing gateway]endpoint={}",
                    endpoint);
        }
        return new UnimplementedTfsGateway(endpoint);
    }

    private static final class UnimplementedTfsGateway implements TfsGateway {

        private final String endpoint;

        private UnimplementedTfsGateway(String endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void forceCloseDir(String fsId, String dirPath) {
            throw new UnsupportedOperationException("Real TFS gateway is not implemented in M1: " + endpoint);
        }
    }
}
