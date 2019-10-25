package com.ctrip.xpipe.service.sso;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.infosec.sso.client.QConfigClient;
import com.ctrip.infosec.sso.client.principal.Assertion;
import com.ctrip.infosec.sso.client.principal.AssertionImpl;
import com.ctrip.infosec.sso.client.principal.AttributePrincipal;
import com.ctrip.infosec.sso.client.principal.AttributePrincipalImpl;
import com.ctrip.infosec.sso.client.util.AESEncrypt;
import com.ctrip.infosec.sso.client.util.AssertionHolder;
import com.ctrip.infosec.sso.client.util.CommonUtils;
import com.ctrip.infosec.sso.client.validate.Cas20ServiceTicketValidator;
import com.ctrip.infosec.sso.client.validate.TicketValidationException;
import com.ctrip.infosec.sso.client.validate.TicketValidator;
import com.ctrip.xpipe.api.sso.SsoConfig;
import com.google.common.base.Strings;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.*;

/**
 * @author chen.zhu
 * <p>
 * Nov 07, 2017
 */
public class  CtripSSOFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(com.ctrip.infosec.sso.client.CtripSSOFilter.class);

    /**
     * Represents the constant for where the assertion will be located in memory.
     */
    public static final String CONST_CAS_ASSERTION = "_const_cas_assertion_";


    private static final int EXPIRE_TIME_ASSERTION = 60 * 60 * 24 * 30;


    private static final String PATH_DELIMITERS = ",; \t\n";

    /**
     * *
     * 通过sso server的接口 缓存已认证的用户信息principal
     */
    private CloseableHttpClient httpClient;


    /**
     * The TicketValidator we will use to validate tickets.
     */
    private TicketValidator validator;

    /**
     * ctrip sso 初始化配置
     */

    private boolean isCluster = false;

    /**
     * The URL to the CAS Server login.
     */
    private String casServerLoginUrl;

    /**
     * The URL prefix to the CAS Server .
     */
    private String casServerUrlPrefix;


    /**
     * Defines the parameter to look for for the artifact.
     */
    private String artifactParameterName = "ticket";

    /**
     * Defines the parameter to look for for the service.
     */
    private String serviceParameterName = "service";

    /**
     * Sets where response.encodeUrl should be called on service urls when constructed.
     */
    private boolean encodeServiceUrl = true;

    /**
     * The name of the server.  Should be in the following format: {protocol}:{hostName}:{port}.
     * Standard ports can be excluded.
     */
    private String serverName;

    /**
     * The exact url of the service.
     */
    private String service;


    /**
     * Specify whether the filter should redirect the user agent after a
     * successful validation to remove the ticket parameter from the query
     * string.
     */
    private boolean redirectAfterValidation = true;


    /**
     * Determines whether an exception is thrown when there is a ticket validation failure.
     */
    private boolean exceptionOnValidationFailure = true;

    /**
     * the paths of parameter ignorePaths is excluded, need not be  authenticated  by sso
     */
    private String[] ignorePaths = null;


    public void setCasServerLoginUrl(String casServerLoginUrl) {
        this.casServerLoginUrl = casServerLoginUrl;
    }

    public void setArtifactParameterName(String artifactParameterName) {
        this.artifactParameterName = artifactParameterName;
    }

    public void setServiceParameterName(String serviceParameterName) {
        this.serviceParameterName = serviceParameterName;
    }

    public void setEncodeServiceUrl(boolean encodeServiceUrl) {
        this.encodeServiceUrl = encodeServiceUrl;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getArtifactParameterName() {
        return artifactParameterName;
    }

    public String getServiceParameterName() {
        return serviceParameterName;
    }

    public boolean isEncodeServiceUrl() {
        return encodeServiceUrl;
    }

    public String getServerName() {
        return serverName;
    }

    public String getService() {
        return service;
    }

    public TicketValidator getValidator() {
        return validator;
    }

    public void setValidator(TicketValidator validator) {
        this.validator = validator;
    }


    public boolean isExceptionOnValidationFailure() {
        return exceptionOnValidationFailure;
    }

    public void setExceptionOnValidationFailure(boolean exceptionOnValidationFailure) {
        this.exceptionOnValidationFailure = exceptionOnValidationFailure;
    }

    public boolean isRedirectAfterValidation() {
        return redirectAfterValidation;
    }

    public void setRedirectAfterValidation(boolean redirectAfterValidation) {
        this.redirectAfterValidation = redirectAfterValidation;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        initIgnorePath(filterConfig);
        initHttpClient();

        String initIsClusterStr = filterConfig.getInitParameter("isCluster");
        if (Strings.isNullOrEmpty(initIsClusterStr)) {
            isCluster = parseBoolean(QConfigClient.getPropertyFromQconfigPrivateFile("isCluster", "true"));
        } else {
            isCluster = parseBoolean(getPropertyFromInitParams(filterConfig, "isCluster", "false"));
        }

        String initServerName = filterConfig.getInitParameter("serverName");
        if (Strings.isNullOrEmpty(initServerName)) {
            setServerName(QConfigClient.getPropertyFromQconfigPrivateFile("serverName", "http://localhost:8080"));
        } else {
            setServerName(getPropertyFromInitParams(filterConfig, "serverName", "http://localhost:8080"));

        }

        logger.trace("Loading serverName property: " + this.serverName);
        setService(getPropertyFromInitParams(filterConfig, "service", null));
        logger.trace("Loading service property: " + this.service);
        setArtifactParameterName(getPropertyFromInitParams(filterConfig, "artifactParameterName", "ticket"));
        logger.trace("Loading artifact parameter name property: " + this.artifactParameterName);
        setServiceParameterName(getPropertyFromInitParams(filterConfig, "serviceParameterName", "service"));
        logger.trace("Loading serviceParameterName property: " + this.serviceParameterName);
        setEncodeServiceUrl(parseBoolean(getPropertyFromInitParams(filterConfig, "encodeServiceUrl", "true")));
        logger.trace("Loading encodeServiceUrl property: " + this.encodeServiceUrl);


        casServerUrlPrefix = QConfigClient.getPropertyFromQconfig("casServerUrlPrefix", "https://cas.uat.qa.nt.ctripcorp.com/caso");

        casServerLoginUrl = casServerUrlPrefix + "/login";


        Cas20ServiceTicketValidator ticketValidator = new Cas20ServiceTicketValidator(casServerUrlPrefix);

        String initEncoding = filterConfig.getInitParameter("encoding");
        if (Strings.isNullOrEmpty(initEncoding)) {
            ticketValidator.setEncoding(QConfigClient.getPropertyFromQconfigPrivateFile("encoding", "UTF-8"));
        } else {
            ticketValidator.setEncoding(getPropertyFromInitParams(filterConfig, "encoding", "UTF-8"));
        }
        setValidator(ticketValidator);

        setExceptionOnValidationFailure(parseBoolean(getPropertyFromInitParams(filterConfig, "exceptionOnValidationFailure", "true")));
        logger.trace("Setting exceptionOnValidationFailure parameter: " + this.exceptionOnValidationFailure);
        setRedirectAfterValidation(parseBoolean(getPropertyFromInitParams(filterConfig, "redirectAfterValidation", "true")));
        logger.trace("Setting redirectAfterValidation parameter: " + this.redirectAfterValidation);

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        if (!checkStopSsoContinue(request, servletResponse)) {
            return;
        }

        if (!this.needFilter(request)) {
            filterChain.doFilter(request, response);
            return;
        }

        /***
         * 这一部风是sso认证逻辑
         */
        Assertion assertion = null;
        if (!isCluster) {
            final HttpSession session = request.getSession(false);
            assertion = session != null ? (Assertion) session.getAttribute(CONST_CAS_ASSERTION) : null;
        } else {
            assertion = getAssertionIncache(request);
        }

        if (assertion != null) {
            AssertionHolder.setAssertion(assertion);
            filterChain.doFilter(request, response);
            return;
        }

        final String serviceUrl = constructServiceUrl(request, response);
        final String ticket = CommonUtils.safeGetParameter(request, getArtifactParameterName());

        if (CommonUtils.isNotBlank(ticket)) {
            validTicket(ticket, request, response);
            if (this.redirectAfterValidation) {
                logger.debug("Redirecting after successful ticket validation.");
                response.sendRedirect(constructServiceUrl(request, response));
                return;
            }
        } else {
            final String urlToRedirectTo = CommonUtils.constructRedirectUrl(this.casServerLoginUrl, getServiceParameterName(), serviceUrl);
            if (logger.isDebugEnabled()) {
                logger.debug("redirecting to \"" + urlToRedirectTo + "\"");
            }
            response.sendRedirect(urlToRedirectTo);
        }


    }

    private boolean needFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return !SsoConfig.excludes(path);
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


    @Override
    public void destroy() {

    }


    protected final String getPropertyFromInitParams(final FilterConfig filterConfig, final String propertyName, final String defaultValue) {
        final String value1 = filterConfig.getInitParameter(propertyName);

        if (CommonUtils.isNotBlank(value1)) {
            logger.info("Property [" + propertyName + "] loaded from FilterConfig.getInitParameter with value [" + value1 + "]");
            return value1;
        }

        final String value2 = filterConfig.getServletContext().getInitParameter(propertyName);

        if (CommonUtils.isNotBlank(value2)) {
            logger.info("Property [" + propertyName + "] loaded from ServletContext.getInitParameter with value [" + value2 + "]");
            return value2;
        }

        logger.info("Property [" + propertyName + "] not found.  Using default value [" + defaultValue + "]");
        return defaultValue;
    }

    protected final boolean parseBoolean(final String value) {
        return ((value != null) && value.equalsIgnoreCase("true"));
    }


    private Assertion getAssertionIncache(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        if (cookies == null) {
            return null;
        }

        String memCacheAssertionID = null;
        String cookieName = generateCookieName(request.getContextPath());

        for (Cookie cookie : cookies) {
            if (cookie.getName().equalsIgnoreCase(cookieName)) {
                memCacheAssertionID = cookie.getValue();
                break;
            }
        }

        Assertion assertionInCache = null;
        try {
            CloseableHttpResponse response = httpClient.execute(new HttpGet(casServerUrlPrefix + "/client/principal?principalId=" + memCacheAssertionID + "&callback=" + serverName));
            String result = EntityUtils.toString(response.getEntity(), "utf-8");
            JSONObject jsonObject = JSON.parseObject(result);

            if (jsonObject.getJSONObject("result") != null) {
                Map user = jsonObject.getJSONObject("result");
                assertionInCache = new AssertionImpl(new AttributePrincipalImpl((String) user.get("name"), user));
            }

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return assertionInCache;
    }

    private void validTicket(String ticket, final HttpServletRequest request, final HttpServletResponse response) throws ServletException {
        if (logger.isDebugEnabled()) {
            logger.debug("Attempting to validate ticket: " + ticket);
        }
        try {
            final Assertion assertion = this.validator.validate(ticket, constructServiceUrl(request, response));

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully authenticated user: " + assertion.getPrincipal().getName());
            }
            AssertionHolder.setAssertion(assertion);

            if (this.isCluster) {

                /***
                 * 用户认证信息写 sso server端接口
                 */

                AttributePrincipal principal = assertion.getPrincipal();
                String uuid = getUUID(principal);

                //设置编码
                try {
                    HttpPost httppost = new HttpPost(casServerUrlPrefix + "/client/principal");

                    Map<String, Object> map = new HashMap<>();
                    map.put("id", uuid);
                    map.put("principal", JSON.toJSONString(principal.getAttributes()));
                    map.put("expire", EXPIRE_TIME_ASSERTION);


                    StringEntity entity = new StringEntity(JSON.toJSONString(map), "UTF-8");
                    entity.setContentEncoding("UTF-8");
                    entity.setContentType("application/json");
                    httppost.setEntity(entity);

                    CloseableHttpResponse httpResponse = httpClient.execute(httppost);
                    String result = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                    JSONObject jsonObject = JSON.parseObject(result);

                    if ((Integer) jsonObject.get("code") == 0) {
                        Cookie cookie = new Cookie(generateCookieName(request.getContextPath()), uuid);
                        cookie.setMaxAge(EXPIRE_TIME_ASSERTION);
                        cookie.setPath(StringUtils.isNotBlank(request.getContextPath()) ? request.getContextPath() : "/");
                        response.addCookie(cookie);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }


            } else {
                /***
                 * 用户认证信息写session
                 */
                request.setAttribute(CONST_CAS_ASSERTION, assertion);
                request.getSession().setAttribute(CONST_CAS_ASSERTION, assertion);
            }


        } catch (final TicketValidationException e) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            logger.warn(e.getMessage(), e);
            if (this.exceptionOnValidationFailure) {
                throw new ServletException(e);
            }
        } catch (Exception e) {
            throw new ServletException(e);
        }

    }

    private String getUUID(AttributePrincipal principal) {
        if (((String) principal.getAttributes().get("company")).equalsIgnoreCase("qunar")) {
            return UUID.randomUUID().toString();
        }
        String userName = principal.getName();
        String distinguishedName = (String) principal.getAttributes().get("distinguishedName");
        String[] dc = distinguishedName.split(",");
        String region = dc[dc.length - 4].split("=")[1];
        return AESEncrypt.hex(AESEncrypt.base64(AESEncrypt.encryptBase("CTRP-SSO-SSO-SSO", userName + "-" + region))) + "-" + UUID.randomUUID().toString();
    }

    private String generateCookieName(String contextPath) {
        if (!Strings.isNullOrEmpty(contextPath) && !contextPath.equals("/")) {
            return contextPath.substring(1) + "_principal";
        }
        return "root_principal";
    }


    protected final String constructServiceUrl(final HttpServletRequest request, final HttpServletResponse response) {
        return constructServiceUrl(request, response, this.service, this.serverName, this.artifactParameterName, this.encodeServiceUrl);
    }

    public String constructServiceUrl(final HttpServletRequest request,
                                             final HttpServletResponse response, final String service, final String serverName, final String artifactParameterName, final boolean encode) {
        if (CommonUtils.isNotBlank(service)) {
            return encode ? response.encodeURL(service) : service;
        }

        final StringBuffer buffer = request.getRequestURL();

        if (CommonUtils.isNotBlank(request.getQueryString())) {
            final int location = request.getQueryString().indexOf(artifactParameterName + "=");

            if (location == 0) {
                final String returnValue = encode ? response.encodeURL(buffer.toString()): buffer.toString();
                return returnValue;
            }

            buffer.append("?");

            if (location == -1) {
                buffer.append(request.getQueryString());
            } else if (location > 0) {
                final int actualLocation = request.getQueryString()
                        .indexOf("&" + artifactParameterName + "=");

                if (actualLocation == -1) {
                    buffer.append(request.getQueryString());
                } else if (actualLocation > 0) {
                    buffer.append(request.getQueryString().substring(0,
                            actualLocation));
                }
            }
        }


        final String returnValue = encode ? response.encodeURL(buffer.toString()) : buffer.toString();

        return returnValue;
    }


    private void initHttpClient() {

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(20);
        connManager.setDefaultMaxPerRoute(connManager.getMaxTotal());
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create().setConnectionManager(connManager);

        httpClientBuilder.setDefaultRequestConfig(
                RequestConfig.custom().setSocketTimeout(3000).setConnectTimeout(3000).setConnectionRequestTimeout(3000).build()
        );

        httpClient = httpClientBuilder.build();
    }

    private void initIgnorePath(FilterConfig filterConfig) {
        String paths = filterConfig.getInitParameter("exclude_paths");
        if (StringUtils.isNotEmpty(paths)) {
            ignorePaths = tokenizeToStringArray(paths, PATH_DELIMITERS, true, true);
        }
        logger.info("IgnorePathFilter initialized, excludePath:{}", ignorePaths);
    }

    private static String[] tokenizeToStringArray(String str, String delimiters, boolean trimTokens, boolean ignoreEmptyTokens) {
        if (str == null) {
            return null;
        }
        StringTokenizer st = new StringTokenizer(str, delimiters);
        List<String> tokens = new ArrayList<>();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (trimTokens) {
                token = token.trim();
            }
            if (!ignoreEmptyTokens || token.length() > 0) {
                tokens.add(token);
            }
        }

        if (CollectionUtils.isEmpty(tokens))
            return null;
        return tokens.toArray(new String[tokens.size()]);
    }
}
