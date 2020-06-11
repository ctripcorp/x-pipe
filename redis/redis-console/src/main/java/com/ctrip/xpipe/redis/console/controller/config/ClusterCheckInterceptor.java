package com.ctrip.xpipe.redis.console.controller.config;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.controller.annotation.ClusterTypeLimit;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.reflect.Method;
import java.util.Map;

public class ClusterCheckInterceptor extends HandlerInterceptorAdapter {

    private MetaCache metaCache;

    public ClusterCheckInterceptor(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        ClusterTypeLimit typeLimit = tryParseLimit(handler);
        if (null == typeLimit) return true;

        String clusterId = tryParseClusterId(request, typeLimit.clusterFieldName());
        if (null == clusterId) return true;

        ClusterType clusterType = tryParseCLusterType(clusterId);
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

    private ClusterType tryParseCLusterType(String clusterId) {
        return metaCache.getClusterType(clusterId);
    }

}
