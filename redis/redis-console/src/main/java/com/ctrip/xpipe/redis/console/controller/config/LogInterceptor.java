package com.ctrip.xpipe.redis.console.controller.config;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 06, 2017
 */
public class LogInterceptor extends HandlerInterceptorAdapter{

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        String uri = request.getRequestURI();
        if(uri.startsWith(AbstractConsoleController.API_PREFIX)){
            logApiRequest(request);
        }

        return super.preHandle(request, response, handler);
    }

    private void logApiRequest(HttpServletRequest request) {

        // Your header-checking code
        String ipAddress = request.getHeader("X-FORWARDED-FOR");
        if (ipAddress == null) {
            ipAddress = request.getRemoteAddr();
        }
        logger.info("[logApiRequest]{}, {}", ipAddress, request.getRequestURI());
    }
}
