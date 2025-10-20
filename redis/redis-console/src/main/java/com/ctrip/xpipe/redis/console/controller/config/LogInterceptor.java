package com.ctrip.xpipe.redis.console.controller.config;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;


/**
 * @author wenchao.meng
 *         <p>
 *         Apr 06, 2017
 */
public class LogInterceptor implements HandlerInterceptor {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        logApiRequest(request, "pre");
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
        logApiRequest(request, "post");

    }

    private void logApiRequest(HttpServletRequest request, String desc) {

        String uri = request.getRequestURI();
        if (uri.startsWith(String.format("%s/%s", AbstractConsoleController.API_PREFIX, "keepers/check"))) {
            return;
        }
        if(uri.startsWith(AbstractConsoleController.API_PREFIX)){
            String ipAddress = getAddresses(request);
            logger.info("[logApiRequest][{}]{}, {}", desc, ipAddress, request.getRequestURI());
        }

    }

    private String getAddresses(HttpServletRequest request) {

        // Your header-checking code
        String xforwardFor = request.getHeader("X-FORWARDED-FOR");
        String remoteAddress = request.getRemoteAddr();
        if(xforwardFor != null){
            return String.format("%s,%s", xforwardFor, remoteAddress);
        }
        return remoteAddress;
    }
}
