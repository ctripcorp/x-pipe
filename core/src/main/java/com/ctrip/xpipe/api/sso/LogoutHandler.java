package com.ctrip.xpipe.api.sso;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * @author lepdou 2016-11-08
 */
public interface LogoutHandler extends Ordered {

    LogoutHandler DEFAULT = ServicesUtil.getLogoutHandler();

    void logout(HttpServletRequest request, HttpServletResponse response);

}
