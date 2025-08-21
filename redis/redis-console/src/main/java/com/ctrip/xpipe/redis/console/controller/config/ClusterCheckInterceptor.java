package com.ctrip.xpipe.redis.console.controller.config;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.controller.annotation.ClusterTypeLimit;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.HandlerMapping;

import java.lang.reflect.Method;
import java.util.Map;

public class ClusterCheckInterceptor implements HandlerInterceptor {

    private MetaCache metaCache;

    private ClusterService clusterService;

    private static final Logger logger = LoggerFactory.getLogger(ClusterCheckInterceptor.class);

    public ClusterCheckInterceptor(MetaCache metaCache, ClusterService clusterService) {
        this.metaCache = metaCache;
        this.clusterService = clusterService;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        ClusterTypeLimit typeLimit = tryParseLimit(handler);
        if (null == typeLimit) return true;

        String clusterId = tryParseClusterId(request, typeLimit.clusterFieldName());
        if (null == clusterId) return true;

        ClusterType clusterType = tryParseClusterType(clusterId);
        for (ClusterType allowType: typeLimit.value()) {
            if (clusterType.equals(allowType)) return true;
        }

        response.sendError(400, "unsupported cluster type");
        return false;
    }

    private ClusterTypeLimit tryParseLimit(Object handler) {
        if (!(handler instanceof HandlerMethod)) return null;

        Method method = ((HandlerMethod) handler).getMethod();
        return method.getAnnotation(ClusterTypeLimit.class);
    }

    private String tryParseClusterId(HttpServletRequest request, String clusterFieldName) {
        if (null == request || null == clusterFieldName) return null;

        Object rawAttributes = request.getAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE);
        if (!(rawAttributes instanceof Map)) return null;

        Map<Object, Object> pathVariables = (Map<Object, Object>) rawAttributes;
        if (!pathVariables.containsKey(clusterFieldName)) return null;

        Object rawValue = pathVariables.get(clusterFieldName);
        if (!(rawValue instanceof String)) return null;

        return (String) rawValue;
    }

    protected ClusterType tryParseClusterType(String clusterId) {
        try {
            return metaCache.getClusterType(clusterId);
        } catch (Exception e) {
            logger.debug("[tryParseClusterType] cache miss", e);
        }

        ClusterTbl clusterTbl = clusterService.find(clusterId);
        if (null == clusterTbl) {
            throw new IllegalArgumentException("unfound cluster " + clusterId);
        }

        return ClusterType.lookup(clusterTbl.getClusterType());
    }

}
