package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class DomainValidateFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(DomainValidateFilter.class);

    private static final String LOCAL_HOST = "localhost";

    public static final String STOP_CHECK_URI = "/stop/domain/check";

    public static final String START_CHECK_URI = "/start/domain/check";

    public static final String HTTP_REQUEST_HEADER_HOST = "HOST";

    private static final String HTTP_PROTOCOL_PREFIX = "http://";

    public static final int BAD_REQUEST_CODE = 400;

    public static final String BAD_REQUEST_MESSAGE_TEMPLATE = "domain name(%s) not match expected(%s)";

    private final AtomicBoolean shouldCheckSetting = new AtomicBoolean(true);

    private final BooleanSupplier shouldCheckConfig;

    private final Supplier<String> expectedDomainName;

    public DomainValidateFilter(BooleanSupplier shouldCheckConfig, Supplier<String> expectedDomainName) {
        this.shouldCheckConfig = shouldCheckConfig;
        this.expectedDomainName = expectedDomainName;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        logger.info("[DomainValidateFilter][filter added]");
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException {
        final HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
        final HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;

        if(startOrStopValidateDomain(httpServletRequest, httpServletResponse)) {
            return;
        }

        if(!shouldCheck()) {
            chain.doFilter(httpServletRequest, httpServletResponse);
            return;
        }

        String host = cleanUrl(httpServletRequest.getHeader(HTTP_REQUEST_HEADER_HOST)).trim();
        // if ip, then return true directly
        if(IpUtils.isValidIpFormat(host) || isLocal(host)) {
            chain.doFilter(httpServletRequest, httpServletResponse);
            return;
        }

        // then check if domain name valid
        if(validateDomain(httpServletResponse, host)) {
            chain.doFilter(httpServletRequest, httpServletResponse);
        } else {
            logger.debug("[domain validate fail]");
        }
    }

    @Override
    public void destroy() {

    }

    private boolean isLocal(String host) {
        return LOCAL_HOST.equalsIgnoreCase(host);
    }

    private boolean validateDomain(HttpServletResponse response, String host) {
        String domain = expectedDomainName.get();
        if(domain == null || domain.isEmpty()) {
            logger.debug("[validateDomain][expected domain name, not found]");
            return true;
        }
        domain = cleanUrl(domain);
        boolean result = domain.equalsIgnoreCase(host);
        if(!result) {
            String message = String.format(BAD_REQUEST_MESSAGE_TEMPLATE, host, domain);
            logger.debug("[validateDomain][domain not match]{}", message);
            try {
                response.sendError(BAD_REQUEST_CODE, message);
            } catch (Exception e) {
                logger.error("[validateDomain][write response error]", e);
            }

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

    @VisibleForTesting
    protected boolean shouldCheck() {
        if(!shouldCheckSetting.get()
                || !shouldCheckConfig.getAsBoolean()) {
            logger.debug("[domain check stopped] {}, {}", shouldCheckSetting.get(), shouldCheckConfig.getAsBoolean());
            return false;
        }
        return true;
    }

    private boolean startOrStopValidateDomain(HttpServletRequest request, ServletResponse servletResponse) {

        String uri = request.getRequestURI();
        boolean action = false;
        if (uri.equalsIgnoreCase(STOP_CHECK_URI)) {
            setShouldCheck(false);
            action = true;
        }

        if (uri.equalsIgnoreCase(START_CHECK_URI)) {
            setShouldCheck(true);
            action = true;
        }

        if (action) {
            try {
                servletResponse.getWriter().write("domain validation status:" + this.shouldCheckSetting.get());
            } catch (IOException e) {
                logger.error("[checkStopSsoContinue]", e);
            }
            return true;
        }
        return false;
    }

    private void setShouldCheck(boolean shouldCheck) {
        this.shouldCheckSetting.set(shouldCheck);
    }

    @VisibleForTesting
    protected boolean getShouldCheckSetting() {
        return shouldCheckSetting.get();
    }
}
