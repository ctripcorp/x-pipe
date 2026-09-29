package com.ctrip.xpipe.redis.comparator.balance;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.springframework.web.client.RestOperations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CMS {@code GetServer} HTTP 调用（D23，内联本模块）。
 * 超时与重试对齐 {@link AbstractService}；不抽 core SPI、不落 ctrip-service。
 */
public class CmsServerGroupProvider extends AbstractService implements ServerGroupProvider {

    public static final String ENTITY_STATUS_WORKING = "WORKING";

    public static final String REQUEST_KEY_GROUP_ID = "group.groupId";

    public static final String REQUEST_KEY_ENTITY_STATUS_IN = "entityStatus@in";

    private final ComparatorConfig config;

    private final FoundationService foundation;

    public CmsServerGroupProvider(ComparatorConfig config) {
        this(config, FoundationService.DEFAULT);
    }

    public CmsServerGroupProvider(ComparatorConfig config, FoundationService foundation) {
        this.config = config;
        this.foundation = foundation;
    }

    @Override
    public List<String> listCiCodes() {
        String url = config.getCmsGetServerUrl();
        String groupId = foundation.getGroupId();
        CmsGetServerRequest request = buildRequest();
        CmsGetServerResponse response;
        try {
            response = restTemplate.postForObject(url, request, CmsGetServerResponse.class);
        } catch (RuntimeException e) {
            logger.error("[listCiCodes] CMS GetServer failed, groupId={}", groupId, e);
            throw e;
        }
        List<String> codes = parseResponse(response);
        logger.info("[listCiCodes] groupId={} size={}", groupId, codes.size());
        return codes;
    }

    @VisibleForTesting
    CmsGetServerRequest buildRequest() {
        CmsGetServerRequest request = new CmsGetServerRequest();
        request.setAccess_token(config.getCmsAccessToken());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put(REQUEST_KEY_GROUP_ID, foundation.getGroupId());
        body.put(REQUEST_KEY_ENTITY_STATUS_IN, Collections.singletonList(ENTITY_STATUS_WORKING));
        request.setRequest_body(body);
        return request;
    }

    @VisibleForTesting
    List<String> parseResponse(CmsGetServerResponse response) {
        if (response == null || !response.isStatus() || response.getData() == null) {
            logger.error("[listCiCodes] CMS GetServer invalid response, groupId={}, response={}",
                    foundation.getGroupId(), response);
            throw new IllegalStateException("CMS GetServer invalid response, groupId=" + foundation.getGroupId());
        }
        List<String> codes = new ArrayList<>();
        for (CmsServer server : response.getData()) {
            if (server == null || StringUtil.isEmpty(server.getCiCode())) {
                continue;
            }
            codes.add(server.getCiCode().trim());
        }
        return codes;
    }

    @VisibleForTesting
    RestOperations restOperations() {
        return restTemplate;
    }

    public static class CmsGetServerRequest {

        private String access_token;

        private Map<String, Object> request_body;

        public String getAccess_token() {
            return access_token;
        }

        public void setAccess_token(String access_token) {
            this.access_token = access_token;
        }

        public Map<String, Object> getRequest_body() {
            return request_body;
        }

        public void setRequest_body(Map<String, Object> request_body) {
            this.request_body = request_body;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CmsGetServerResponse {

        private boolean status;

        private List<CmsServer> data;

        public boolean isStatus() {
            return status;
        }

        public void setStatus(boolean status) {
            this.status = status;
        }

        public List<CmsServer> getData() {
            return data;
        }

        public void setData(List<CmsServer> data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return "CmsGetServerResponse{status=" + status
                    + ", dataSize=" + (data == null ? -1 : data.size()) + "}";
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CmsServer {

        private String ciCode;

        public String getCiCode() {
            return ciCode;
        }

        public void setCiCode(String ciCode) {
            this.ciCode = ciCode;
        }
    }
}
