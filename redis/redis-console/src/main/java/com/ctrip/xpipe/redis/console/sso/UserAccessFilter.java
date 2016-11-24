package com.ctrip.xpipe.redis.console.sso;

import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author lepdou 2016-11-08
 */
public class UserAccessFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(UserAccessFilter.class);

    private UserInfoHolder userInfoHolder;
    private ConsoleConfig consoleConfig;

    public UserAccessFilter(UserInfoHolder userInfoHolder, ConsoleConfig consoleConfig) {
        this.userInfoHolder = userInfoHolder;
        this.consoleConfig = consoleConfig;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        //do nothing
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {

        //ignore api request and static resources
        if (! isRequestIgnorable(((HttpServletRequest) request).getRequestURI())) {
            String currentAccessUser = userInfoHolder.getUser().getUserId();

            if (!isCurrentAccessUserHitWhiteList(currentAccessUser)) {
                HttpServletResponse resp = (HttpServletResponse) response;
                resp.sendError(HttpStatus.SC_UNAUTHORIZED);
                return;
            }
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        //do nothing
    }

    private boolean isCurrentAccessUserHitWhiteList(String user) {
        Set<String> userWhiteList = consoleConfig.getConsoleUserAccessWhiteList();

        if (CollectionUtils.isEmpty(userWhiteList)){
            logger.warn("Request forbid because user white list is empty");
            return false;
        }

        if (userWhiteList.contains("*")){
            return true;
        }else {
            return userWhiteList.contains(user);
        }

    }
    
    private boolean isRequestIgnorable(String uri) {
    	return uri.startsWith("/api") || uri.endsWith(".html")
    			|| uri.endsWith(".css") || uri.endsWith(".js")
    			|| uri.endsWith(".woff") || uri.endsWith(".ttf");
    }
}
