package com.ctrip.xpipe.service.sso;

import com.ctrip.infosec.sso.client.CtripSSOFilter;
import com.ctrip.xpipe.api.sso.SsoConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/3/23
 */
public class XPipeSSOFilter extends CtripSSOFilter {

    private static final Logger logger = LoggerFactory.getLogger(XPipeSSOFilter.class);
    private static final String STOP_SSO_URI = "/stopsso";
    private static final String START_SSO_URI = "/startsso";
    private static final String TOKEN_HEADER = "token";
    private final SsoControlConfig ssoControlConfig;

    public XPipeSSOFilter() {
        this(new SsoControlConfig());
    }

    XPipeSSOFilter(SsoControlConfig ssoControlConfig) {
        this.ssoControlConfig = ssoControlConfig;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        if (!checkStopSsoContinue(request, response)) {
            return;
        }
        if (!this.needFilter(request)) {
            filterChain.doFilter(request, response);
            return;
        }

        super.doFilter(servletRequest, servletResponse, filterChain);
    }

    private boolean needFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return !SsoConfig.excludes(path);
    }

    private boolean checkStopSsoContinue(HttpServletRequest request, HttpServletResponse response) {

        String uri = request.getRequestURI();
        if (!isSsoControlAction(uri)) {
            return true;
        }

        if (!isValidToken(request)) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            try {
                response.getWriter().write("invalid token");
            } catch (IOException e) {
                logger.error("[checkStopSsoContinue][write invalid token failed]", e);
            }
            return false;
        }

        SsoConfig.stopsso = STOP_SSO_URI.equalsIgnoreCase(uri);
        try {
            response.getWriter().write("sso stop status:" + SsoConfig.stopsso);
        } catch (IOException e) {
            logger.error("[checkStopSsoContinue]", e);
        }
        return false;
    }

    private boolean isSsoControlAction(String uri) {
        return STOP_SSO_URI.equalsIgnoreCase(uri) || START_SSO_URI.equalsIgnoreCase(uri);
    }

    private boolean isValidToken(HttpServletRequest request) {
        String expectedToken = ssoControlConfig.getSsoControlToken();
        String actualToken = request.getHeader(TOKEN_HEADER);
        return expectedToken != null && !expectedToken.isEmpty() && expectedToken.equals(actualToken);
    }

}
