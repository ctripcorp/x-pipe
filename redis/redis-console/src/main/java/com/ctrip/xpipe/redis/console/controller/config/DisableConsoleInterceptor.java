package com.ctrip.xpipe.redis.console.controller.config;

import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class DisableConsoleInterceptor extends HandlerInterceptorAdapter {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        if(request.getRequestURI().startsWith("/console/")) {
            response.sendError(404, "cannot access db");
            return false;
        } else {
            return true;
        }
    }
}
