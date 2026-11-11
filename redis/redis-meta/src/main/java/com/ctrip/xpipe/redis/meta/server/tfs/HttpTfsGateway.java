package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.tfs.proto.TfsStatus;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Real TFS Gateway over HTTP POST + JSON via {@link RestOperations}.
 * Host/appId are read from {@link MetaServerConfig} on every call (D23 hot reload).
 * Business failure ({@code status.error_code != 0}) returns {@code false}; transport errors throw.
 */
public class HttpTfsGateway implements TfsGateway {

    public static final String CONTENT_TYPE_JSON = "application/json";

    public static final String METRIC_TYPE = "call.tfs";

    public static final String API_FORCE_CLOSE_DIR = "ForceCloseDir";

    private static final String FIELD_FS_ID = "fs_id";
    private static final String FIELD_DIR_PATH = "dir_path";
    private static final String FIELD_POD_IP = "pod_ip";
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_ERROR_CODE = "error_code";
    private static final String FIELD_MESSAGE = "message";

    private static final MediaType JSON = MediaType.APPLICATION_JSON;

    private static final Logger logger = LoggerFactory.getLogger(HttpTfsGateway.class);

    private static final RestOperations SHARED_REST = RestTemplateFactory.createCommonsHttpRestTemplate(
            10, 100,
            TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI,
            TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI,
            0);

    private final MetaServerConfig config;
    private final String fixedHost;
    private final long fixedAppId;
    private final RestOperations restOperations;

    private MetricProxy metricProxy = MetricProxy.DEFAULT;

    public HttpTfsGateway(MetaServerConfig config) {
        this(config, SHARED_REST);
    }

    HttpTfsGateway(MetaServerConfig config, RestOperations restOperations) {
        if (config == null) {
            throw new IllegalArgumentException("metaServerConfig required");
        }
        this.config = config;
        this.fixedHost = null;
        this.fixedAppId = -1L;
        this.restOperations = restOperations == null ? SHARED_REST : restOperations;
    }

    /**
     * Host-only convenience (tests / Factory overload). No Config hot reload.
     */
    public HttpTfsGateway(String host, long appId) {
        this(host, appId, SHARED_REST);
    }

    HttpTfsGateway(String host, long appId, RestOperations restOperations) {
        this.config = null;
        this.fixedHost = host;
        this.fixedAppId = appId;
        this.restOperations = restOperations == null ? SHARED_REST : restOperations;
    }

    MetaServerConfig getConfig() {
        return config;
    }

    @VisibleForTesting
    protected void setMetricProxy(MetricProxy metricProxy) {
        this.metricProxy = metricProxy;
    }

    @Override
    public boolean forceCloseDir(String fsId, String dirPath, String podIp) throws Exception {
        long startTime = System.currentTimeMillis();
        String host = resolveHost();
        long appId = resolveAppId();
        if (StringUtil.isEmpty(host)) {
            throw new IllegalStateException("empty TFS gateway host");
        }
        String url = buildUrl(host, appId, fsId);
        boolean success = false;
        Integer errorCode = null;
        String message = null;
        try {
            String body = JsonCodec.INSTANCE.encode(createRequestBody(fsId, dirPath, podIp));
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(JSON);
            ResponseEntity<String> response = restOperations.exchange(
                    url, HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
            TfsStatus status = parseStatus(response.getBody());
            if (status == null) {
                message = "missing status";
                success = false;
            } else {
                errorCode = status.getErrorCode();
                message = status.getMessage();
                success = errorCode == 0;
            }
            return success;
        } finally {
            long endTime = System.currentTimeMillis();
            logger.info("[forceCloseDir]url={}, fsId={}, dirPath={}, podIp={}, success={}, errorCode={}, message={}, cost={}ms",
                    url, fsId, dirPath, podIp, success, errorCode, message, endTime - startTime);
            tryMetric(API_FORCE_CLOSE_DIR, success, startTime, endTime);
        }
    }

    private Map<String, String> createRequestBody(String fsId, String dirPath, String podIp) {
        Map<String, String> request = new LinkedHashMap<>();
        request.put(FIELD_FS_ID, fsId == null ? "" : fsId);
        request.put(FIELD_DIR_PATH, dirPath == null ? "" : dirPath);
        request.put(FIELD_POD_IP, podIp == null ? "" : podIp);
        return request;
    }

    private TfsStatus parseStatus(String responseBody) {
        if (StringUtil.isEmpty(responseBody)) {
            return null;
        }

        Map<String, Object> response = JsonCodec.INSTANCE.decode(responseBody,
                new GenericTypeReference<Map<String, Object>>() {
                });
        if (response == null) {
            return null;
        }

        Object statusNode = response.get(FIELD_STATUS);
        if (!(statusNode instanceof Map)) {
            return null;
        }

        Map<?, ?> status = (Map<?, ?>) statusNode;
        Object errorCode = status.get(FIELD_ERROR_CODE);
        if (!(errorCode instanceof Number)) {
            return null;
        }

        Object message = status.get(FIELD_MESSAGE);
        return new TfsStatus(((Number) errorCode).intValue(), message instanceof String ? (String) message : "");
    }

    private void tryMetric(String api, boolean isSuccess, long startTime, long endTime) {
        try {
            MetricData metricData = new MetricData(METRIC_TYPE, null, null, null);
            metricData.setTimestampMilli(startTime);
            metricData.addTag("api", api);
            metricData.setValue(endTime - startTime);
            metricData.addTag("status", isSuccess ? "SUCCESS" : "FAIL");
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (Throwable th) {
            logger.debug("[tryMetric] fail", th);
        }
    }

    private String resolveHost() {
        return config != null ? config.getTfsGatewayHost() : fixedHost;
    }

    private long resolveAppId() {
        return config != null ? config.getTfsGatewayAppId() : fixedAppId;
    }

    static String buildUrl(String host, long appId, String fsId) {
        String base = host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
        String encodedFsId = URLEncoder.encode(fsId == null ? "" : fsId, StandardCharsets.UTF_8)
                .replace("+", "%20");
        return base + "/tfs/app/" + appId + "/fs/" + encodedFsId + "/TFSGateway/ForceCloseDir";
    }
}
