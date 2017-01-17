package com.ctrip.xpipe.service.sso;


import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.sso.LogoutHandler;

import java.io.IOException;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * @author lepdou 2016-11-08
 */
public class CtripLogoutHandler implements LogoutHandler {

    private Config config = Config.DEFAULT;

    @Override
    public void logout(HttpServletRequest request, HttpServletResponse response) {

        HttpSession session = request.getSession(false);
        if (session != null) {
            session.invalidate();
        }

        Cookie cookie = new Cookie("memCacheAssertionID", null);
        cookie.setMaxAge(0);
        cookie.setPath(request.getContextPath() + "/");
        response.addCookie(cookie);

        String casServerUrlPrefix = config.get(SSOConfigurations.KEY_CAS_SERVER_URL_PREFIX);
        @SuppressWarnings("unused")
		String casRegisterServerName = config.get(SSOConfigurations.KEY_CAS_REGISTER_SERVER_NAME);

        try {
            response.sendRedirect(casServerUrlPrefix + "/logout?service=");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
