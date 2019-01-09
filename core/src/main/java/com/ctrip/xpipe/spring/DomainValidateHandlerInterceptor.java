package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.function.Supplier;

public class DomainValidateHandlerInterceptor implements HandlerInterceptor {

    public static final String HTTP_REQUEST_HEADER_HOST = "HOST";

    private static final String HTTP_PROTOCOL_PREFIX = "http://";

    public static final int BAD_REQUEST_CODE = 400;

    public static final String BAD_REQUEST_MESSAGE_TEMPLATE = "domain name(%s) not match expected(%s)";

    private final Supplier<String> expectedDomainName;

    public DomainValidateHandlerInterceptor(Supplier<String> expectedDomainName) {
        this.expectedDomainName = expectedDomainName;
    }

    @Override
    public boolean preHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object o) throws Exception {
        String host = cleanUrl(httpServletRequest.getHeader(HTTP_REQUEST_HEADER_HOST)).trim();
        // if ip, then return true directly
        if(IpUtils.isValidIpFormat(host)) {
            return true;
        }

        // then check if domain name valid
        String domain = cleanUrl(expectedDomainName.get());
        boolean result = domain.equalsIgnoreCase(host);
        if(!result) {
            httpServletResponse.sendError(BAD_REQUEST_CODE, String.format(BAD_REQUEST_MESSAGE_TEMPLATE, host, domain));
        }
        return result;
    }

    private String cleanUrl(String urlStr) {
        return removePortIfNeeded(removeProtocolIfNeeded(urlStr));
    }

    @VisibleForTesting
    protected String removeProtocolIfNeeded(String urlStr) {
        if(urlStr.startsWith(HTTP_PROTOCOL_PREFIX)) {
            return urlStr.replace(HTTP_PROTOCOL_PREFIX, "");
        }
        return urlStr;
    }

    @VisibleForTesting
    protected String removePortIfNeeded(String host) {
        return host.split("\\s*:\\s*")[0];
    }

    @Override
    public void postHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object o, ModelAndView modelAndView) throws Exception {

    }

    @Override
    public void afterCompletion(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object o, Exception e) throws Exception {

    }
}
