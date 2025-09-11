package com.ctrip.xpipe.spring;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import jakarta.servlet.FilterChain;

import static com.ctrip.xpipe.spring.DomainValidateFilter.HTTP_REQUEST_HEADER_HOST;
import static com.ctrip.xpipe.spring.DomainValidateFilter.START_CHECK_URI;
import static com.ctrip.xpipe.spring.DomainValidateFilter.STOP_CHECK_URI;
import static org.mockito.Mockito.*;

public class DomainValidateFilterTest {

    private Logger logger = LoggerFactory.getLogger(DomainValidateFilterTest.class);

    private DomainValidateFilter filter;

    private FilterChain filterChain = new MockFilterChain();

    private String domain;

    private boolean shouldCheck = true;

    @Before
    public void beforeDomainValidateHandlerInterceptorTest() {
        domain = "";
        filter = new DomainValidateFilter(()->shouldCheck, ()->domain);
    }

    @Test
    public void preHandleIp() throws Exception {
        domain = "localhost";
        String ip = "10.0.0.1:8080";
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader(HTTP_REQUEST_HEADER_HOST, ip);
        filter.doFilter(request, response, filterChain);
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void preHandleDomain() throws Exception {
        domain = "localhost";
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader(HTTP_REQUEST_HEADER_HOST, domain);
        filter.doFilter(request, response, filterChain);
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void preHandleDomainPort() throws Exception {
        domain = "localhost";
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader(HTTP_REQUEST_HEADER_HOST, domain + ":8080");
        filter.doFilter(request, response, filterChain);
        Assert.assertEquals(200, response.getStatus());
    }


    @Test
    public void removeProtocolIfNeeded() {
        String target = "http://10.2.1.1:8080";
        Assert.assertEquals("10.2.1.1:8080", filter.removeProtocolIfNeeded(target));
    }

    @Test
    public void removePortIfNeeded() {
        String target = "10.2.1.1:8080";
        Assert.assertEquals("10.2.1.1", filter.removePortIfNeeded(target));

        target = "10.2.1.1";
        Assert.assertEquals("10.2.1.1", filter.removePortIfNeeded(target));

        target = "domain.name:8080";
        Assert.assertEquals("domain.name", filter.removePortIfNeeded(target));

        target = "domain.name";
        Assert.assertEquals("domain.name", filter.removePortIfNeeded(target));
    }

    @Test
    public void testShouldCheck() throws Exception {
        filter = spy(filter);
        shouldCheck = false;
        Assert.assertFalse(filter.shouldCheck());

        domain = "localhost";
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader(HTTP_REQUEST_HEADER_HOST, "error.domain");
        filter.doFilter(request, response, filterChain);

        Assert.assertEquals(200, response.getStatus());
        verify(filter, never()).removePortIfNeeded(domain);
        verify(filter, never()).removeProtocolIfNeeded(domain);
        verify(filter, never()).removePortIfNeeded("error.domain");
    }

    @Test
    public void testSwitchForValidation() throws Exception {
        Assert.assertTrue(filter.getShouldCheckSetting());

        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader(HTTP_REQUEST_HEADER_HOST, "127.0.0.1:8080");
        request.setRequestURI(STOP_CHECK_URI);
        filter.doFilter(request, response, filterChain);

        String content = response.getContentAsString();
        logger.info("[content] {}", content);

        Assert.assertFalse(filter.getShouldCheckSetting());

        request.setRequestURI(START_CHECK_URI);
        response = new MockHttpServletResponse();
        filter.doFilter(request, response, filterChain);

        content = response.getContentAsString();
        logger.info("[content] {}", content);
        Assert.assertTrue(filter.getShouldCheckSetting());
    }

    @Test
    public void testLocalHostCall() throws Exception {
        domain = "xpipe.meta.ctripcorp.com";
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader(HTTP_REQUEST_HEADER_HOST, "localhost:8080");
        filter.doFilter(request, response, filterChain);

        Assert.assertEquals(200, response.getStatus());
    }
}