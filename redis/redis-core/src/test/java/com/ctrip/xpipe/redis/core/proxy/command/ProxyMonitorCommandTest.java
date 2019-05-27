package com.ctrip.xpipe.redis.core.proxy.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.monitor.*;
import com.ctrip.xpipe.simpleserver.Server;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.List;

public class ProxyMonitorCommandTest extends AbstractRedisTest {

    private Server server;

    private HostPort frontend1 = HostPort.fromString("10.26.188.107:47862"), frontend2 = HostPort.fromString("10.14.13.212:443");

    private HostPort backend1 = HostPort.fromString("10.26.188.107:80"), backend2 = HostPort.fromString("10.14.13.212:47862");

    private String template1 = "ESTAB      0      0             10.26.188.107:47862         10.15.206.22:443";

    private String template2 = "skmem:(r0,rb235128,t0,tb87380,f0,w0,o0,bl0) cubic wscale:7,10 rto:458 rtt:176.498/45.981 ato:40 mss:1448 cwnd:10 send 656.3Kbps rcv_rtt:175 rcv_space:26883";

    private String template3 = "SYN-SENT   0      1             10.26.188.107:49766        10.26.190.167:9092";

    private String template4 = "skmem:(r0,rb87380,t0,tb87380,f4294966016,w1280,o0,bl0) cubic rto:4000 mss:524 cwnd:1 ssthresh:7 unacked:1 retrans:1/2 lost:1";


    @After
    public void afterProxyMonitorCommandTest() throws Exception {
        if(server != null) {
            server.stop();
        }
    }

    @Test
    public void testProxyMonitorSocketStats() throws Exception {
        TunnelSocketStatsResult sample1 = new TunnelSocketStatsResult(tunnelId("front1", "back1"),
                socketStatsResult(Lists.newArrayList(template1, template2)),
                socketStatsResult(Lists.newArrayList(template3, template4)));
        TunnelSocketStatsResult sample2 = new TunnelSocketStatsResult(tunnelId("front2", "back2"),
                socketStatsResult(Lists.newArrayList(template3, template4)),
                socketStatsResult(Lists.newArrayList(template1, template2)));
        server = startServer(new ArrayParser(new Object[]{sample1.format(), sample2.format()}).format().toString(Charset.defaultCharset()));
        Endpoint target = getEndpoint();
        TunnelSocketStatsResult[] result = new AbstractProxyMonitorCommand.ProxyMonitorSocketStatsCommand(
                getXpipeNettyClientKeyedObjectPool().getKeyPool(target), scheduled).execute().get();

        Assert.assertEquals(2, result.length);
        Assert.assertEquals(sample1, result[0]);
        Assert.assertEquals(sample2, result[1]);
    }

    @Test
    public void testProxyMonitorTraficStats() throws Exception {
        TunnelTrafficResult sample1 = new TunnelTrafficResult(tunnelId("front1", "back1"),
                sessionTrafficResult(), sessionTrafficResult());
        TunnelTrafficResult sample2 = new TunnelTrafficResult(tunnelId("front1", "back1"),
                sessionTrafficResult(), sessionTrafficResult());
        server = startServer(new ArrayParser(new Object[]{sample1.format(), sample2.format()})
                .format().toString(Charset.defaultCharset()));
        Endpoint target = getEndpoint();
        TunnelTrafficResult[] result = new AbstractProxyMonitorCommand.ProxyMonitorTrafficStatsCommand(
                getXpipeNettyClientKeyedObjectPool().getKeyPool(target), scheduled).execute().get();

        Assert.assertEquals(2, result.length);
        Assert.assertEquals(sample1, result[0]);
        Assert.assertEquals(sample2, result[1]);
    }

    @Test
    public void testProxyMonitorTunnelStats() throws Exception {
        long timestamp = System.currentTimeMillis();
        TunnelStatsResult sample1 = new TunnelStatsResult(tunnelId("front1", "back1"), "Tunnel-Established", timestamp - 1000, timestamp - 990, frontend1, backend1);
        TunnelStatsResult sample2 = new TunnelStatsResult(tunnelId("front2", "back2"), "Tunnel-Closed", frontend2, backend2, timestamp - 1000, timestamp - 990, timestamp, "FRONTEND");
        server = startServer(new ArrayParser(new Object[]{sample1.toArrayObject(), sample2.toArrayObject()}).format().toString(Charset.defaultCharset()));
        TunnelStatsResult[] results = new AbstractProxyMonitorCommand.ProxyMonitorTunnelStatsCommand(
                getXpipeNettyClientKeyedObjectPool().getKeyPool(getEndpoint()), scheduled).execute().get();

        Assert.assertEquals(2, results.length);
        Assert.assertEquals(sample1, results[0]);
        Assert.assertEquals(sample2, results[1]);
    }

    @Test
    public void testProxyMonitorPingStats() throws Exception {
        long timestamp = System.currentTimeMillis();
        int N = 100;
        Object[] samples = new Object[N];
        for(int i = 0; i < N; i++) {
            samples[i] = new PingStatsResult(timestamp - randomInt(), timestamp - randomInt(),
                    localHostport(randomPort()), localHostport(randomPort())).toArrayObject();
        }
        server = startServer(new ArrayParser(samples).format().toString(Charset.defaultCharset()));

        PingStatsResult[] results = new AbstractProxyMonitorCommand.ProxyMonitorPingStatsCommand(
                getXpipeNettyClientKeyedObjectPool().getKeyPool(getEndpoint()), scheduled).execute().get();

        Assert.assertEquals(N, results.length);
        for(int i = 0; i < N; i++) {
            Assert.assertEquals(PingStatsResult.parse(samples[i]), results[i]);
        }
    }

    //manually
    @Ignore
    @Test
    public void testProxyMonitorSocketStatsManual() throws Exception {
        objectPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultProxyEndpoint("PROXYTCP://10.2.131.200:80"));

        Command command = new AbstractProxyMonitorCommand.ProxyMonitorPingStatsCommand(objectPool, scheduled);
        command.future().addListener(new CommandFutureListener<PingStatsResult[]>() {
            @Override
            public void operationComplete(CommandFuture<PingStatsResult[]> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()) {
                    logger.error("[update][{}]", getClass().getSimpleName(), commandFuture.cause());
                }
                logger.info("{}", commandFuture.getNow());
            }
        });
        command.execute();
        logger.info("[ping-stats]");
        command = new AbstractProxyMonitorCommand.ProxyMonitorTunnelStatsCommand(objectPool, scheduled);
        command.future().addListener(new CommandFutureListener<TunnelStatsResult[]>() {
            @Override
            public void operationComplete(CommandFuture<TunnelStatsResult[]> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()) {
                    logger.error("[update][{}]", getClass().getSimpleName(), commandFuture.cause());
                }
                logger.info("{}", commandFuture.getNow());
            }
        });
        command.execute();
        logger.info("[tunnel-stats]");
        command = new AbstractProxyMonitorCommand.ProxyMonitorSocketStatsCommand(objectPool, scheduled);
        command.future().addListener(new CommandFutureListener<TunnelSocketStatsResult[]>() {
            @Override
            public void operationComplete(CommandFuture<TunnelSocketStatsResult[]> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()) {
                    logger.error("[update][{}]", getClass().getSimpleName(), commandFuture.cause());
                }
                logger.info("{}", commandFuture.getNow());
            }
        });
        command.execute();
        logger.info("[socket-stats]");
        sleep(5000);
    }

    @Ignore
    @Test
    public void testManual() throws Exception {
        objectPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultProxyEndpoint("PROXYTCP://10.2.131.200:80"));
        new SocketStatsResultsUpdater().update();
        sleep(200);
        Assert.assertTrue(socketStatsResults.size() > 1);
        logger.info("{}", socketStatsResults);
    }

    private abstract class AbstractInfoUpdater<T> {

        public void update() {
            Command<T[]> command = getCommand();
            command.future().addListener(new CommandFutureListener<T[]>() {
                @Override
                public void operationComplete(CommandFuture<T[]> commandFuture) throws Exception {
                    if(!commandFuture.isSuccess()) {
                        logger.error("[update][{}]", getClass().getSimpleName(), commandFuture.cause());
                    }
                    updateRelevantField(commandFuture.getNow());
                }
            });
            command.execute();
        }

        protected abstract void updateRelevantField(T[] updates);

        protected abstract Command<T[]> getCommand();
    }

    private List<TunnelSocketStatsResult> socketStatsResults = Lists.newArrayList();

    private SimpleObjectPool<NettyClient> objectPool;

    private class SocketStatsResultsUpdater extends AbstractInfoUpdater<TunnelSocketStatsResult> {

        @Override
        protected void updateRelevantField(TunnelSocketStatsResult[] updates) {
            synchronized (this) {
                socketStatsResults = Lists.newArrayList(updates);
            }
        }

        @Override
        protected Command<TunnelSocketStatsResult[]> getCommand() {
            return new AbstractProxyMonitorCommand.ProxyMonitorSocketStatsCommand(objectPool, scheduled);
        }
    }

    private Endpoint getEndpoint() {
        return new DefaultEndPoint("127.0.0.1", server.getPort());
    }

    private String tunnelId(String front, String back) {
        String source = "source", dest = "dest";
        return String.format("%s-%s-%s-%s", source, front, back, dest);
    }

    private SocketStatsResult socketStatsResult(List<String> strs) {
        return new SocketStatsResult(strs);
    }

    private SessionTrafficResult sessionTrafficResult() {
        return new SessionTrafficResult(System.currentTimeMillis(), randomInt(), randomInt(), randomInt(), randomInt());
    }
}