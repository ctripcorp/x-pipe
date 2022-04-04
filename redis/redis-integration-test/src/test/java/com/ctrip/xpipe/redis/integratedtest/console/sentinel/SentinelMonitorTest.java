package com.ctrip.xpipe.redis.integratedtest.console.sentinel;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.redis.integratedtest.console.AbstractXPipeDrTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author lishanglin
 * date 2021/4/7
 */
public class SentinelMonitorTest extends AbstractXPipeDrTest {

    private Map<String, List<HostPort>> dcSentinels = new HashMap<String, List<HostPort>>() {{
        put("jq", Arrays.asList(new HostPort("127.0.0.1", 5000), new HostPort("127.0.0.1", 5001), new HostPort("127.0.0.1", 5002)));
        put("oy", Arrays.asList(new HostPort("127.0.0.1", 17170), new HostPort("127.0.0.1", 17171), new HostPort("127.0.0.1", 17172)));
    }};

    protected void startSimpleXPipeDR() throws Exception {
        super.startSimpleXPipeDR();

        dcSentinels.values().forEach(sentinels -> {
            sentinels.forEach(sentinel -> {
                startSentinel(sentinel.getPort());
            });
        });
    }

    @Test
    public void testAddSentinels() throws Exception {
        startSimpleXPipeDR();
        waitForAllSentinelReady("jq", "127.0.0.1", 6379);
    }

    @Test
    public void testMasterConsistent() throws Exception {
        startSimpleXPipeDR();
        String masterName = SentinelUtil.getSentinelMonitorName("cluster-dr", "cluster-dr-shard1", "jq");
        waitForAllSentinelReady("jq", LOCAL_HOST, 6379);

        Assert.assertEquals(6379,sentinelMaster(LOCAL_HOST, 5000, masterName).getPort());

        sentinelRemove(LOCAL_HOST, 5000, masterName);
        sentinelMonitor(LOCAL_HOST, 5000, masterName, new HostPort(LOCAL_HOST, 7379));

        Assert.assertEquals(7379,sentinelMaster(LOCAL_HOST, 5000, masterName).getPort());

        waitForSentinelMaster(LOCAL_HOST, 5000, masterName, LOCAL_HOST, 6379, 60000);
    }

    @Test
    public void testMonitorChangeAfterDrSwitch() throws Exception {
        startSimpleXPipeDR();

        // check migration system
        waitForServerRespAsExpected("http://127.0.0.1:8080/api/migration/migration/system/health/status", RetMessage.class, RetMessage.createSuccessMessage(), 60000);

        // do migration
        tryMigration("http://127.0.0.1:8080", "cluster-dr", "jq", "oy");

        waitForAllSentinelNoMonitor("jq");
        waitForAllSentinelReady("oy", "127.0.0.1", 7379);
    }

    protected void waitForAllSentinelReady(String dc, String masterIp, int masterPort) throws Exception {
        for (HostPort sentinel : dcSentinels.get(dc)) {
            waitForSentinelMaster(sentinel.getHost(), sentinel.getPort(),
                    SentinelUtil.getSentinelMonitorName("cluster-dr", "cluster-dr-shard1", dc),
                    masterIp, masterPort, 300000);
        }
    }

    protected void waitForAllSentinelNoMonitor(String dc) throws Exception {
        for (HostPort sentinel : dcSentinels.get(dc)) {
            waitForSentinelNoMonitor(sentinel.getHost(), sentinel.getPort(),
                    SentinelUtil.getSentinelMonitorName("cluster-dr", "cluster-dr-shard1", dc),
                    300000);
        }
    }

    protected void waitForSentinelMaster(String host, int port, String monitorName, String masterIp, int masterPort, int waitTimeMilli) throws Exception {
        waitConditionUntilTimeOut(() -> {
            try {
                HostPort master = sentinelMaster(host, port, monitorName);
                logger.info("[waitForSentinelMaster][{}][{}] {} master {}", host, port, monitorName, master);
                return master.equals(new HostPort(masterIp, masterPort));
            } catch (Throwable th) {
                logger.info("[waitForSentinelMaster][{}][{}] sentinel master cmd fail", host, port, th);
                return false;
            }
        }, waitTimeMilli, 2000);
    }

    protected void waitForSentinelNoMonitor(String host, int port, String monitorName, int waitTimeMilli) throws Exception {
        waitConditionUntilTimeOut(() -> {
            try {
                HostPort master = sentinelMaster(host, port, monitorName);
                logger.info("[waitForSentinelNoMonitor][{}][{}] {} master {}", host, port, monitorName, master);
                return false;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RedisError && e.getCause().getMessage().startsWith("ERR No such master with that name")) {
                    return true;
                } else {
                    logger.info("[waitForSentinelNoMonitor][{}][{}] sentinel master cmd fail", host, port, e);
                    return false;
                }
            } catch (Throwable th) {
                logger.info("[waitForSentinelNoMonitor][{}][{}] sentinel master cmd fail", host, port, th);
                return false;
            }
        }, waitTimeMilli, 2000);
    }

    protected HostPort sentinelMaster(String host, int port, String monitorName) throws Exception {
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint(host, port));
        AbstractSentinelCommand.SentinelMaster sentinelMaster = new AbstractSentinelCommand
                .SentinelMaster(clientPool, scheduled, monitorName);
        return sentinelMaster.execute().get().getHostPort();
    }

    protected String sentinelRemove(String host, int port, String monitorName) throws Exception {
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint(host, port));
        AbstractSentinelCommand.SentinelRemove remove = new AbstractSentinelCommand
                .SentinelRemove(clientPool, monitorName, scheduled);
        return remove.execute().get();
    }

    protected String sentinelMonitor(String host, int port, String monitorName, HostPort master) throws Exception {
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint(host, port));
        AbstractSentinelCommand.SentinelMonitor monitor = new AbstractSentinelCommand
                .SentinelMonitor(clientPool, scheduled, monitorName, master, 2);
        return monitor.execute().get();
    }

}
