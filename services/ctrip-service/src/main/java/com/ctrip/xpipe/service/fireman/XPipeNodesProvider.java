package com.ctrip.xpipe.service.fireman;

import com.ctrip.framework.fireman.container.application.AppNode;
import com.ctrip.framework.fireman.remote.http.NodesProvider;
import com.ctrip.framework.fireman.remote.http.cms.DefaultNodesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/11/24
 */
public class XPipeNodesProvider extends DefaultNodesProvider implements NodesProvider {

    private FiremanConfig config = new FiremanConfig();

    private static final String PARAM_QUERIES = "request_body";

    private static final String QUERY_POOL_IN = "poolId@in";

    private Logger logger = LoggerFactory.getLogger(XPipeNodesProvider.class);

    @Override
    public List<AppNode> fetchNodes(Map<String, Object> requestBody) {
        Set<String> poolIds = config.getFiremanRelatedPools();
        if (null != poolIds && !poolIds.isEmpty()) {
            Map<Object, Object> queries = new HashMap<>();
            if (requestBody.get(PARAM_QUERIES) instanceof Map) {
                queries.putAll((Map) requestBody.get(PARAM_QUERIES));
            }
            queries.put(QUERY_POOL_IN, poolIds);
            requestBody.put(PARAM_QUERIES, queries);
        }

        logger.info("[fetchNodes] request: {}", requestBody);
        return super.fetchNodes(requestBody);
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
