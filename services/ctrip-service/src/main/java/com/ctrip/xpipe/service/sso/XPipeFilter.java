package com.ctrip.xpipe.service.sso;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 16, 2017
 */
import com.ctrip.xpipe.api.sso.SsoConfig;
import credis.java.client.CacheProvider;
import credis.java.client.util.CacheFactory;
import org.jasig.cas.client.authentication.DefaultGatewayResolverImpl;
import org.jasig.cas.client.authentication.GatewayResolver;
import org.jasig.cas.client.util.AbstractCasFilter;
import org.jasig.cas.client.util.CommonUtils;
import org.jasig.cas.client.validation.Assertion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

public class XPipeFilter extends AbstractCasFilter {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private String casServerLoginUrl;
    private boolean renew = false;
    private boolean gateway = false;
    private boolean useSession = false;
    private CacheProvider provider = null;
    private GatewayResolver gatewayStorage = new DefaultGatewayResolverImpl();

    public XPipeFilter() {
    }

    protected void initInternal(FilterConfig filterConfig) throws ServletException {

        if(!this.isIgnoreInitConfiguration()) {
            super.initInternal(filterConfig);
            this.setCasServerLoginUrl(this.getPropertyFromInitParams(filterConfig, "casServerLoginUrl", (String)null));
            this.log.trace("Loaded CasServerLoginUrl parameter: " + this.casServerLoginUrl);
            this.setRenew(this.parseBoolean(this.getPropertyFromInitParams(filterConfig, "renew", "false")));
            this.log.trace("Loaded renew parameter: " + this.renew);
            this.setGateway(this.parseBoolean(this.getPropertyFromInitParams(filterConfig, "gateway", "false")));
            this.log.trace("Loaded gateway parameter: " + this.gateway);
            this.setGateway(this.parseBoolean(this.getPropertyFromInitParams(filterConfig, "useSession", "false")));
            this.log.trace("Loaded useSession parameter: " + this.useSession);
            String gatewayStorageClass = this.getPropertyFromInitParams(filterConfig, "gatewayStorageClass", (String)null);
            if(gatewayStorageClass != null) {
                try {
                    this.gatewayStorage = (GatewayResolver)Class.forName(gatewayStorageClass).newInstance();
                } catch (Exception var4) {
                    this.log.error(var4, var4);
                    throw new ServletException(var4);
                }
            }

            String redisClusterName = this.getPropertyFromInitParams(filterConfig, "redisClusterName", "localhost:11211");
            this.log.trace("Setting redisClusterName parameter: " + redisClusterName);
            this.setProvider(CacheFactory.GetProvider(redisClusterName));
        }
    }

    public void init() {

        super.init();
        CommonUtils.assertNotNull(this.casServerLoginUrl, "casServerLoginUrl cannot be null.");
    }

    private boolean needFilter(HttpServletRequest request) {

        String path = request.getRequestURI();
        return !SsoConfig.excludes(path);
    }

    public final void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest)servletRequest;
        HttpServletResponse response = (HttpServletResponse)servletResponse;

        if (!checkStopSsoContinue(request, servletResponse)) {
            return;
        }

        if(!this.needFilter(request)) {
            filterChain.doFilter(request, response);
        } else {
            Cookie[] cookies = request.getCookies();
            String memCacheAssertionID = null;
            if(cookies != null) {
                Cookie[] var8 = cookies;
                int var9 = cookies.length;

                for(int var10 = 0; var10 < var9; ++var10) {
                    Cookie cookie = var8[var10];
                    if(cookie.getName().equalsIgnoreCase("memCacheAssertionID")) {
                        memCacheAssertionID = cookie.getValue();
                    }
                }
            }

            Assertion assertionInMemcache = null;

            try {
                if(memCacheAssertionID != null) {
                    byte[] value = this.provider.get(memCacheAssertionID.getBytes());
                    assertionInMemcache = (Assertion)(new ObjectInputStream(new ByteArrayInputStream(value))).readObject();
                }
            } catch (Exception var15) {
                this.log.error(var15.getMessage());
            }

            if(this.useSession && assertionInMemcache != null) {
                request.getSession().setAttribute("_const_cas_assertion_", assertionInMemcache);
            }

            Assertion assertion = null;
            if(assertionInMemcache != null) {
                assertion = assertionInMemcache;
            } else if(this.useSession) {
                HttpSession session = request.getSession(false);
                assertion = session != null?(Assertion)session.getAttribute("_const_cas_assertion_"):null;
            }

            if(assertion != null) {
                request.setAttribute("_const_cas_assertion_", assertion);
                filterChain.doFilter(request, response);
            } else {
                String serviceUrl = this.constructServiceUrl(request, response);
                String ticket = CommonUtils.safeGetParameter(request, this.getArtifactParameterName());
                boolean wasGatewayed = this.gatewayStorage.hasGatewayedAlready(request, serviceUrl);
                if(!CommonUtils.isNotBlank(ticket) && !wasGatewayed) {
                    this.log.debug("no ticket and no assertion found");
                    String modifiedServiceUrl;
                    if(this.gateway) {
                        this.log.debug("setting gateway attribute in session");
                        modifiedServiceUrl = this.gatewayStorage.storeGatewayInformation(request, serviceUrl);
                    } else {
                        modifiedServiceUrl = serviceUrl;
                    }

                    if(this.log.isDebugEnabled()) {
                        this.log.debug("Constructed service url: " + modifiedServiceUrl);
                    }

                    String urlToRedirectTo = CommonUtils.constructRedirectUrl(this.casServerLoginUrl, this.getServiceParameterName(), modifiedServiceUrl, this.renew, this.gateway);
                    if(this.log.isDebugEnabled()) {
                        this.log.debug("redirecting to \"" + urlToRedirectTo + "\"");
                    }

                    response.sendRedirect(urlToRedirectTo);
                } else {
                    filterChain.doFilter(request, response);
                }
            }
        }
    }


    private boolean checkStopSsoContinue(HttpServletRequest request, ServletResponse servletResponse) {

        String uri = request.getRequestURI();
        boolean action = false;
        if (uri.equalsIgnoreCase("/stopsso")) {
            SsoConfig.stopsso = true;
            action = true;
        }

        if (uri.equalsIgnoreCase("/startsso")) {
            SsoConfig.stopsso = false;
            action = true;
        }

        if (action) {
            try {
                servletResponse.getWriter().write("sso stop status:" + SsoConfig.stopsso);
            } catch (IOException e) {
                logger.error("[checkStopSsoContinue]", e);
            }
            return false;
        }
        return true;
    }


    public final void setRenew(boolean renew) {
        this.renew = renew;
    }

    public final void setGateway(boolean gateway) {
        this.gateway = gateway;
    }

    public final void setCasServerLoginUrl(String casServerLoginUrl) {
        this.casServerLoginUrl = casServerLoginUrl;
    }

    public final void setGatewayStorage(GatewayResolver gatewayStorage) {
        this.gatewayStorage = gatewayStorage;
    }

    public void setUseSession(boolean useSession) {
        this.useSession = useSession;
    }

    public void setProvider(CacheProvider provider) {
        this.provider = provider;
    }
}
