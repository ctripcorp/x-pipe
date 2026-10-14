package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.keeper.container.HostNameVerification;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

public class HostNameVerifyFilterTest extends AbstractTest {

    private static final String LOCAL_HOST_NAME = "keepercontainer-pod-a-0";
    private static final String LOCAL_IP = "10.0.0.1";

    private HostNameVerifyFilter filter;
    private MockHttpServletResponse response;

    @Before
    public void beforeHostNameVerifyFilterTest() {
        filter = new HostNameVerifyFilter(() -> LOCAL_HOST_NAME, () -> LOCAL_IP);
        response = new MockHttpServletResponse();
    }

    @Test
    public void testMatchedHostNamePassesThrough() throws Exception {
        MockFilterChain chain = doFilter(LOCAL_HOST_NAME);

        Assert.assertNotNull("请求应被放行", chain.getRequest());
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testHostNameIsCaseInsensitive() throws Exception {
        MockFilterChain chain = doFilter(LOCAL_HOST_NAME.toUpperCase());

        Assert.assertNotNull("hostname 比对不应受大小写影响", chain.getRequest());
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testMismatchedHostNameIsRejected() throws Exception {
        MockFilterChain chain = doFilter("keepercontainer-pod-b-0");

        Assert.assertNull("打错目标的请求不能进入业务逻辑", chain.getRequest());
        Assert.assertEquals(HostNameVerification.SC_MISDIRECTED_REQUEST, response.getStatus());
        Assert.assertTrue("错误信息应同时给出目标与实际，便于定位",
                response.getErrorMessage().contains("keepercontainer-pod-b-0")
                        && response.getErrorMessage().contains(LOCAL_HOST_NAME));
    }

    @Test
    public void testMissingHeaderPassesThrough() throws Exception {
        // 物理机时代的存量调用方不带该头，必须放行
        MockFilterChain chain = doFilter(null);

        Assert.assertNotNull(chain.getRequest());
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testBlankHeaderPassesThrough() throws Exception {
        MockFilterChain chain = doFilter("");

        Assert.assertNotNull(chain.getRequest());
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testUnavailableLocalHostNamePassesThrough() throws Exception {
        // 本机 hostname 探测失败时放行：不能因为身份取不到就让整台容器的管理面瘫掉
        filter = new HostNameVerifyFilter(() -> null, () -> LOCAL_IP);
        MockFilterChain chain = doFilter("keepercontainer-pod-b-0");

        Assert.assertNotNull(chain.getRequest());
        Assert.assertEquals(200, response.getStatus());
    }

    private MockFilterChain doFilter(String targetHostName) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/keepers/election/reset");
        if (null != targetHostName) {
            request.addHeader(HostNameVerification.HEADER_TARGET_HOST_NAME, targetHostName);
        }
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request, response, chain);
        return chain;
    }
}
