package com.ctrip.xpipe.redis.integratedtest.console.dr;

import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.integratedtest.console.AbstractXPipeDrTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;


/**
 * @author lishanglin
 * date 2021/1/21
 */
public class DRTest extends AbstractXPipeDrTest {

    @Test
    public void testNormalMigration() throws Exception {
        startSimpleXPipeDR();

        // check migration system
        waitForServerRespAsExpected("http://127.0.0.1:8080/api/migration/migration/system/health/status", RetMessage.class, RetMessage.createSuccessMessage(), 60000);

        // do migration
        tryMigration("http://127.0.0.1:8080", "cluster-dr", "jq", "oy");

        // check result
        waitForServerRespAsExpected("http://127.0.0.1:8081/api/health/127.0.0.1/6379", String.class, "\"HEALTHY\"", 30000);
    }

    @Test
    public void testMigrationWhenOriginDcMetaserverDown() throws Exception {
        startSimpleXPipeDR();

        // check migration system
        waitForServerRespAsExpected("http://127.0.0.1:8081/api/migration/migration/system/health/status", RetMessage.class, RetMessage.createSuccessMessage(), 60000);

        stopServer(jqMetaServer);


        if (jqMetaServer.isProcessAlive()) Assert.fail("jq metaserver is still alive after kill");

        // do migration
        tryMigration("http://127.0.0.1:8081", "cluster-dr", "jq", "oy");

        // check primary dc up
        waitForServerRespAsExpected("http://127.0.0.1:8081/api/health/127.0.0.1/7379", String.class, "\"HEALTHY\"", 30000);
        waitForRedisRole("127.0.0.1", 7379, Server.SERVER_ROLE.MASTER, 15000);

        // recover origin primary dc metaserver
        jqMetaServer = startMetaServer("jq", "http://127.0.0.1:8080", zkJQ, IdcUtil.JQ_METASERVER_PORT, dcInfos);

        // wait for repl recover
        waitForServerRespAsExpected("http://127.0.0.1:8081/api/health/127.0.0.1/6379", String.class, "\"HEALTHY\"", 120000);
    }

    @Test
    public void testMultiConsoleMigrationTogether() throws Exception {
        startSimpleXPipeDR();

        // check migration system
        waitForServerRespAsExpected("http://127.0.0.1:8081/api/migration/migration/system/health/status", RetMessage.class, RetMessage.createSuccessMessage(), 60000);

        long ticketId = tryPrepareMigration("http://127.0.0.1:8080", "cluster-dr", "jq", "oy");
        Arrays.asList("http://127.0.0.1:8080", "http://127.0.0.1:8081").forEach(host -> {
            executors.execute(() -> {
                tryDoMigration(host, ticketId);
            });
        });

        waitForServerRespAsExpected("http://127.0.0.1:8081/api/health/127.0.0.1/6379", String.class, "\"HEALTHY\"", 30000);
    }

    @Test
    public void testBeaconAutoMigration() throws Exception {
        startSimpleXPipeDR();

        // check migration system
        waitForServerRespAsExpected("http://127.0.0.1:8081/api/migration/migration/system/health/status", RetMessage.class, RetMessage.createSuccessMessage(), 60000);

        Set<MonitorGroupMeta> groups = buildMonitorMeta(new HashMap<String, Map<String, Set<HostPort>>>() {{
            put("jq", Collections.singletonMap("cluster-dr-shard1", Collections.singleton(new HostPort("127.0.0.1", 6379))));
            put("oy", Collections.singletonMap("cluster-dr-shard1", Collections.singleton(new HostPort("127.0.0.1", 7379))));
        }}, "jq");

        tryBeaconAutoMigration("http://127.0.0.1:8081", "cluster-dr", groups, Collections.singleton("cluster-dr-shard1+jq"));
        waitForServerRespAsExpected("http://127.0.0.1:8081/api/health/127.0.0.1/6379", String.class, "\"HEALTHY\"", 30000);
    }

    @Test
    public void testBeaconForceMigration() throws Exception {
        startSimpleXPipeDR();

        // check migration system
        waitForServerRespAsExpected("http://127.0.0.1:8081/api/migration/migration/system/health/status", RetMessage.class, RetMessage.createSuccessMessage(), 60000);

        tryBeaconForceMigration("http://127.0.0.1:8081", "cluster-dr", "oy");
        waitForServerRespAsExpected("http://127.0.0.1:8081/api/health/127.0.0.1/6379", String.class, "\"HEALTHY\"", 30000);
    }

}
