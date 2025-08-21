package com.ctrip.xpipe.redis.console.controller.config;


import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.servlet.HandlerInterceptor;

public class DisableConsoleInterceptor implements HandlerInterceptor {

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
