package com.ctrip.xpipe.redis.console.manual;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.MonitorServiceFactory;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.config.model.BeaconClusterRoute;
import com.ctrip.xpipe.redis.console.config.model.BeaconOrgRoute;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultMonitorClusterManager;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2024/1/25
 */
public class BeaconRouteTest extends AbstractTest {

    private Map<Long, DefaultMonitorClusterManager> orgMonitorMap;

    @Test
    public void test() {
        String property = "[]";
        List<BeaconOrgRoute> beaconOrgRoutes = JsonCodec.INSTANCE.decode(property, new GenericTypeReference<List<BeaconOrgRoute>>() {});

        this.orgMonitorMap = beaconOrgRoutes.stream().collect(
                Collectors.toMap(BeaconOrgRoute::getOrgId, orgRoute -> {
                    List<MonitorService> monitorServices = orgRoute.getClusterRoutes()
                            .stream()
                            .map(this::constructMonitorService)
                            .collect(Collectors.toList());
                    return new DefaultMonitorClusterManager(null, 100, monitorServices, 0);
                }));

        DefaultMonitorClusterManager clusterManager = this.orgMonitorMap.get(0L);
        MonitorService service = clusterManager.getService("test_beacon_probe2");
        logger.info("[test] {}", service.getName());
    }

    private MonitorService constructMonitorService(BeaconClusterRoute route) {
        return MonitorServiceFactory.DEFAULT.build(route.getName(), route.getHost(), route.getWeight());
    }

}
