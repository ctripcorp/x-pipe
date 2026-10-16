package com.ctrip.xpipe.redis.comparator.balance;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.comparator.balance.CmsServerGroupProvider.CmsGetServerRequest;
import com.ctrip.xpipe.redis.comparator.balance.CmsServerGroupProvider.CmsGetServerResponse;
import com.ctrip.xpipe.redis.comparator.balance.CmsServerGroupProvider.CmsServer;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * T-LB.2：请求体与解析契约；不访问网络。
 */
public class CmsServerGroupProviderTest extends AbstractTest {

    @Test
    public void testBuildRequestJsonKeys() throws Exception {
        CmsServerGroupProvider provider = new CmsServerGroupProvider(
                ConfigurableCmsConfig.configured(), new StubFoundation());
        CmsGetServerRequest request = provider.buildRequest();
        Assert.assertEquals("token", request.getAccess_token());
        Assert.assertEquals("group1", request.getRequest_body().get(CmsServerGroupProvider.REQUEST_KEY_GROUP_ID));
        Assert.assertEquals(Collections.singletonList(CmsServerGroupProvider.ENTITY_STATUS_WORKING),
                request.getRequest_body().get(CmsServerGroupProvider.REQUEST_KEY_ENTITY_STATUS_IN));

        String json = new ObjectMapper().writeValueAsString(request);
        Assert.assertTrue(json.contains("\"access_token\""));
        Assert.assertTrue(json.contains("\"request_body\""));
        Assert.assertTrue(json.contains("\"group.groupId\""));
        Assert.assertTrue(json.contains("\"entityStatus@in\""));
        Assert.assertTrue(json.contains("WORKING"));
    }

    @Test
    public void testParseResponseSkipsBlankCiCode() {
        CmsServerGroupProvider provider = new CmsServerGroupProvider(
                ConfigurableCmsConfig.configured(), new StubFoundation());
        List<String> codes = provider.parseResponse(okResponse(" host-b ", "", "host-a"));
        Assert.assertEquals(Arrays.asList("host-b", "host-a"), codes);
    }

    @Test
    public void testInvalidResponseThrows() {
        CmsServerGroupProvider provider = new CmsServerGroupProvider(
                ConfigurableCmsConfig.configured(), new StubFoundation());
        try {
            provider.parseResponse(null);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("invalid response"));
        }
        CmsGetServerResponse failed = new CmsGetServerResponse();
        failed.setStatus(false);
        try {
            provider.parseResponse(failed);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("invalid response"));
        }
    }

    @Test
    public void testTimeoutAlignedWithAbstractService() {
        CmsServerGroupProvider provider = new CmsServerGroupProvider(new ComparatorConfig());
        Assert.assertTrue(provider instanceof AbstractService);
        Assert.assertNotNull(provider.restOperations());
        Assert.assertFalse(provider.restOperations() instanceof RestTemplate);
    }

    private static CmsGetServerResponse okResponse(String... ciCodes) {
        CmsGetServerResponse response = new CmsGetServerResponse();
        response.setStatus(true);
        List<CmsServer> data = new java.util.ArrayList<>();
        for (String code : ciCodes) {
            CmsServer server = new CmsServer();
            server.setCiCode(code);
            data.add(server);
        }
        response.setData(data);
        return response;
    }

    static final class ConfigurableCmsConfig extends ComparatorConfig {

        static ConfigurableCmsConfig configured() {
            return new ConfigurableCmsConfig();
        }

        @Override
        public String getCmsAccessToken() {
            return "token";
        }

        @Override
        public String getCmsGetServerUrl() {
            return "http://cms.example/GetServer";
        }
    }

    static final class StubFoundation implements FoundationService {

        @Override
        public String getDataCenter() {
            return "jq";
        }

        @Override
        public String getAppId() {
            return "test";
        }

        @Override
        public String getLocalIp() {
            return "127.0.0.1";
        }

        @Override
        public String getHostName() {
            return "me";
        }

        @Override
        public String getGroupId() {
            return "group1";
        }

        @Override
        public String getRegion() {
            return "sha";
        }

        @Override
        public int getOrder() {
            return 0;
        }
    }
}
