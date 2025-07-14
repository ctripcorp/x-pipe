package com.ctrip.framework.xpipe.redis.servlet.spring;

import com.ctrip.framework.xpipe.redis.servlet.ProxyServlet;
import jakarta.servlet.*;

import java.io.IOException;

public class ProxyFilter implements Filter {

    private ProxyServlet proxyServlet;
    public ProxyFilter() throws ServletException {
        proxyServlet = new ProxyServlet();
        proxyServlet.init();
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {

    }
}
