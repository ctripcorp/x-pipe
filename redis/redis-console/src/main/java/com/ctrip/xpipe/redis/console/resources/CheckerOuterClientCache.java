package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.OuterClientCache;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClientException;

import java.util.Collections;
import java.util.Map;

/**
 * @author lishanglin
 * date 2022/7/18
 */
public class CheckerOuterClientCache implements OuterClientCache {

    private CheckerConsoleService service;

    private CheckerConfig config;

    private static final Logger logger = LoggerFactory.getLogger(CheckerOuterClientCache.class);

    public CheckerOuterClientCache(CheckerConsoleService service, CheckerConfig config) {
        this.service = service;
        this.config = config;
    }

    @Override
    public OuterClientService.ClusterInfo getClusterInfo(String clusterName) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, OuterClientService.ClusterInfo> getAllActiveDcClusters(String activeDc) {
        try {
            return service.loadAllActiveDcOneWayClusterInfo(config.getConsoleAddress(), activeDc);
        } catch (RestClientException e) {
            logger.warn("[getAllOneWayClusters] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[getAllOneWayClusters] fail", th);
        }

        return Collections.emptyMap();
    }

}
