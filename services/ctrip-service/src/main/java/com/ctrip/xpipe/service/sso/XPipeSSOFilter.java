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

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        if (!checkStopSsoContinue(request, servletResponse)) {
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

    private boolean checkStopSsoContinue(HttpServletRequest request, ServletResponse servletResponse) {

        String uri = request.getRequestURI();
        boolean action = false;
        if (uri.equalsIgnoreCase("/stopsso")) {
            SsoConfig.stopsso = true;
            action = true;
        }

        if (uri.equalsIgnoreCase("/startsso")) {
            SsoConfig.stopsso = false;
            action = true;
        }

        if (action) {
            try {
                servletResponse.getWriter().write("sso stop status:" + SsoConfig.stopsso);
            } catch (IOException e) {
                logger.error("[checkStopSsoContinue]", e);
            }
            return false;
        }
        return true;
    }

}
