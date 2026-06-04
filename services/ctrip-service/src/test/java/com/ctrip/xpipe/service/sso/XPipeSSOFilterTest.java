package com.ctrip.xpipe.service.sso;

import com.ctrip.xpipe.api.sso.SsoConfig;
import com.ctrip.xpipe.service.AbstractServiceTest;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class XPipeSSOFilterTest extends AbstractServiceTest {

    private static final String TOKEN = "token-for-test";

    private SsoControlConfig ssoControlConfig;
    private XPipeSSOFilter filter;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private FilterChain filterChain;
    private StringWriter responseWriter;

    @Before
    public void setUp() throws Exception {
        ssoControlConfig = mock(SsoControlConfig.class);
        when(ssoControlConfig.getSsoControlToken()).thenReturn(TOKEN);
        filter = new XPipeSSOFilter(ssoControlConfig);

        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        filterChain = mock(FilterChain.class);
        responseWriter = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(responseWriter, true));
        SsoConfig.stopsso = false;
    }

    @Test
    public void testStopSsoWithValidToken() throws Exception {
        when(request.getRequestURI()).thenReturn("/stopsso");
        when(request.getHeader("token")).thenReturn(TOKEN);

        filter.doFilter(request, response, filterChain);

        assertTrue(SsoConfig.stopsso);
        assertEquals("sso stop status:true", responseWriter.toString());
        verify(filterChain, never()).doFilter(any(), any());
        verify(response, never()).setStatus(HttpServletResponse.SC_FORBIDDEN);
    }

    @Test
    public void testStartSsoWithValidToken() throws Exception {
        SsoConfig.stopsso = true;
        when(request.getRequestURI()).thenReturn("/startsso");
        when(request.getHeader("token")).thenReturn(TOKEN);

        filter.doFilter(request, response, filterChain);

        assertFalse(SsoConfig.stopsso);
        assertEquals("sso stop status:false", responseWriter.toString());
        verify(filterChain, never()).doFilter(any(), any());
        verify(response, never()).setStatus(HttpServletResponse.SC_FORBIDDEN);
    }

    @Test
    public void testStopSsoWithInvalidToken() throws Exception {
        when(request.getRequestURI()).thenReturn("/stopsso");
        when(request.getHeader("token")).thenReturn("wrong-token");

        filter.doFilter(request, response, filterChain);

        assertFalse(SsoConfig.stopsso);
        assertEquals("invalid token", responseWriter.toString());
        verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
        verify(filterChain, never()).doFilter(any(), any());
    }

    @Test
    public void testNonControlPathShouldPassWhenExcluded() throws Exception {
        when(request.getRequestURI()).thenReturn("/health");

        filter.doFilter(request, response, filterChain);

        verify(filterChain).doFilter(request, response);
    }
}
