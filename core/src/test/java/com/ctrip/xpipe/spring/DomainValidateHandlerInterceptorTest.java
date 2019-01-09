package com.ctrip.xpipe.spring;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static com.ctrip.xpipe.spring.DomainValidateHandlerInterceptor.HTTP_REQUEST_HEADER_HOST;

public class DomainValidateHandlerInterceptorTest {

    private DomainValidateHandlerInterceptor interceptor;

    private String domain;

    @Before
    public void beforeDomainValidateHandlerInterceptorTest() {
        domain = "";
        interceptor = new DomainValidateHandlerInterceptor(()->domain);
    }

    @Test
    public void preHandleIp() throws Exception {
        domain = "localhost";
        String ip = "10.0.0.1:8080";
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader(HTTP_REQUEST_HEADER_HOST, ip);
        interceptor.preHandle(request, response, null);
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void preHandleDomain() throws Exception {
        domain = "localhost";
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader(HTTP_REQUEST_HEADER_HOST, domain);
        interceptor.preHandle(request, response, null);
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void preHandleDomainPort() throws Exception {
        domain = "localhost";
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader(HTTP_REQUEST_HEADER_HOST, domain + ":8080");
        interceptor.preHandle(request, response, null);
        Assert.assertEquals(200, response.getStatus());
    }


    @Test
    public void removeProtocolIfNeeded() {
        String target = "http://10.2.1.1:8080";
        Assert.assertEquals("10.2.1.1:8080", interceptor.removeProtocolIfNeeded(target));
    }

    @Test
    public void removePortIfNeeded() {
        String target = "10.2.1.1:8080";
        Assert.assertEquals("10.2.1.1", interceptor.removePortIfNeeded(target));

        target = "10.2.1.1";
        Assert.assertEquals("10.2.1.1", interceptor.removePortIfNeeded(target));

        target = "domain.name:8080";
        Assert.assertEquals("domain.name", interceptor.removePortIfNeeded(target));

        target = "domain.name";
        Assert.assertEquals("domain.name", interceptor.removePortIfNeeded(target));
    }
}