package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.simpleserver.Server;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class CheckTrueMasterTest extends AbstractCheckerTest {

    private CheckTrueMaster checkTrueMaster;

    @Mock
    private AlertManager alertManager;

    private String monitorName = "shard1";
    private Set<HostPort> masterSentinels;
    private HostPort master = new HostPort("127.0.0.1", randomPort());

//    Set<SentinelHello> hellos,
//    String sentinelMonitorName, Set<HostPort> sentinels,
//    HostPort metaMaster, List<HostPort> shardInstances,
//    Map<ClusterType, String[]> clusterTypeSentinelConfig

    @Before
    public void init() throws Exception {
        checkTrueMaster=new CheckTrueMaster(new SentinelHelloCollectContext(),alertManager,null,null);
        checkTrueMaster.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool()).setScheduled(scheduled);
        masterSentinels = Sets.newHashSet(
                new HostPort("127.0.0.1", 5000),
                new HostPort("127.0.0.1", 5001),
                new HostPort("127.0.0.1", 5002),
                new HostPort("127.0.0.1", 5003),
                new HostPort("127.0.0.1", 5004)
        );
    }

    @Test
    public void checkTrueMastersTest() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        SentinelHelloCollectContext context = new SentinelHelloCollectContext(instance.getCheckInfo(), new HashSet<>(), monitorName, masterSentinels, null, Lists.newArrayList(), null);
        checkTrueMaster.setContext(context);

//        meta master and hello masters all empty
        Set<SentinelHello> hellos = new HashSet<>();
        try {
            checkTrueMaster.execute().get();
            Assert.assertNull(context.getTrueMaster());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof MasterNotFoundException);
        }

//        meta master and hello masters consistent
        HostPort helloMaster = new HostPort(LOCAL_HOST, randomPort());
        context.setMetaMaster(helloMaster);
        SentinelHello hello1 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), helloMaster, monitorName);
        SentinelHello hello2 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), helloMaster, monitorName);
        SentinelHello hello3 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), helloMaster, monitorName);
        SentinelHello hello4 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), helloMaster, monitorName);
        SentinelHello hello5 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), helloMaster, monitorName);
        hellos = Sets.newHashSet(hello1, hello2, hello3, hello4, hello5);
        context.setHellos(hellos);
        context.setTrueMaster(null);
        context.setAllMasters(new HashSet<>());

        checkTrueMaster.execute().get();
        Assert.assertEquals(helloMaster, context.getTrueMaster());

//        meta master null and hello master consistent
        context.setTrueMaster(null);
        context.setAllMasters(new HashSet<>());
        context.setMetaMaster(null);
        checkTrueMaster.execute().get();
        Assert.assertEquals(helloMaster, context.getTrueMaster());

//        meta master inconsistent with hello master

        //double masters
        HostPort metaMaster = new HostPort(LOCAL_HOST, randomPort());
        Server metaMasterServer = startServer(metaMaster.getPort(), "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":0\r\n*0\r\n");
        Server helloMasterServer = startServer(helloMaster.getPort(), "*3\r\n" +
                "$6\r\nmaster\r\n" +
                ":224016677\r\n" +
                "*2\r\n" +
                "*3\r\n" +
                "$9\r\n" +
                "127.0.0.1\r\n" +
                "$5\r\n" +
                "20001\r\n" +
                "$9\r\n" +
                "224016497\r\n" +
                "*3\r\n" +
                "$9\r\n" +
                "127.0.0.1\r\n" +
                "$4\r\n" +
                "6380\r\n" +
                "$9\r\n" +
                "224016497\r\n"
        );

        metaMasterServer.stop();
        //single master

//      the master do not has all slaves
        try {
            context.setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST,20001),new HostPort(LOCAL_HOST,20002),new HostPort(LOCAL_HOST,helloMaster.getPort())));
            context.setMetaMaster(metaMaster);
            context.setTrueMaster(null);
            context.setAllMasters(new HashSet<>());
            checkTrueMaster.execute().get();
            Assert.assertNull(context.getTrueMaster());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof MasterNotFoundException);
        }
//      the master has all slaves
        try {
            context.setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST,20001),new HostPort(LOCAL_HOST,helloMaster.getPort())));
            context.setMetaMaster(metaMaster);
            context.setTrueMaster(null);
            context.setAllMasters(new HashSet<>());
            checkTrueMaster.execute().get();
            Assert.assertEquals(helloMaster, context.getTrueMaster());
        } catch (Exception e) {
            Assert.fail();
        }

        helloMasterServer.stop();
        //no masters
        helloMaster = new HostPort(LOCAL_HOST, randomPort());
        hello1 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), helloMaster, monitorName);
        hello2 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), helloMaster, monitorName);
        hello3 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), helloMaster, monitorName);
        hello4 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), helloMaster, monitorName);
        hello5 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), helloMaster, monitorName);
        hellos = Sets.newHashSet(hello1, hello2, hello3, hello4, hello5);

        try {
            context.setTrueMaster(null);
            context.setAllMasters(new HashSet<>());
            context.setHellos(hellos);
            checkTrueMaster.execute().get();
            Assert.assertNull(context.getTrueMaster());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof MasterNotFoundException);
        }
    }



}
