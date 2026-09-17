package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.core.keeper.container.HostNameVerification;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * 校验直连命令是否打对了本容器：请求头带的目标 hostname 与本机 hostname 不一致时拒绝。
 * <p>
 * 防的是 pod 重建后 IP 被回收再分配，而调用方缓存未刷新，导致命令按旧 IP 打到新租户身上。
 * removeKeeper、IO 限流、resetKeeper 都是写操作，打错即事故。
 * <p>
 * 不带该请求头的调用一律放行，兼容物理机时代的存量调用方 —— 物理机以 IP 为身份，
 * IP 不会易主，本就不需要这层校验。
 */
public class HostNameVerifyFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(HostNameVerifyFilter.class);

    public static final String MISMATCH_MESSAGE_TEMPLATE =
            "request targets keepercontainer(%s) but this is (%s), refresh address cache and retry";

    private final Supplier<String> localHostName;

    public HostNameVerifyFilter(Supplier<String> localHostName) {
        this.localHostName = localHostName;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        logger.info("[HostNameVerifyFilter][filter added]");
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        String target = request.getHeader(HostNameVerification.HEADER_TARGET_HOST_NAME);
        if (StringUtil.isEmpty(target)) {
            chain.doFilter(request, response);
            return;
        }

        String local = localHostName.get();
        if (StringUtil.isEmpty(local)) {
            // 本机 hostname 取不到时无法判断，放行而非拒绝：宁可漏掉一次校验，
            // 也不要因为身份探测失败让整台容器的管理面全部不可用
            logger.warn("[doFilter][local hostname unavailable, skip verify]target:{}", target);
            chain.doFilter(request, response);
            return;
        }

        if (target.equalsIgnoreCase(local)) {
            chain.doFilter(request, response);
            return;
        }

        String message = String.format(MISMATCH_MESSAGE_TEMPLATE, target, local);
        logger.warn("[doFilter][hostname mismatch]{} {}", request.getRequestURI(), message);
        response.sendError(HostNameVerification.SC_MISDIRECTED_REQUEST, message);
    }

    @Override
    public void destroy() {
    }
}
