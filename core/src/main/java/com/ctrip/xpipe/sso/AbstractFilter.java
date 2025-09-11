package com.ctrip.xpipe.sso;

import com.ctrip.xpipe.api.sso.SsoConfig;

import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 16, 2017
 */
public abstract class AbstractFilter implements jakarta.servlet.Filter {


    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }


    protected boolean needFilter(HttpServletRequest request) {

        String path = request.getRequestURI();
        return !exclude(path);
    }


    protected boolean exclude(String uri) {

        return SsoConfig.excludes(uri);
    }


    @Override
    public void destroy() {

    }
}
