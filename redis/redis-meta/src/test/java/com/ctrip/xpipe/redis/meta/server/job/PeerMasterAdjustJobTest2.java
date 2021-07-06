package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClientFactory;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoReplicationCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyFactory;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl.XpipeRedisProxy;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.PublishCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.RedisInfo;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class PeerMasterAdjustJobTest2 extends AbstractMetaServerTest {
    protected Server redisServer;

    protected int port = randomPort();

    class PeerMaster {
        RedisMeta meta;
        private static final String TEMP_CRDT_INFO_BASIC = "peer%d_host:%s\r\n" + "peer%d_port:%d\r\n" + "peer%d_gid:%d\r\n";
        private static final String TEMP_CRDT_INFO_SERVERS = "peer%d_proxy_type:xpipe_proxy\r\n" + "peer%d_proxy_servers:%s\r\n";
        private static final String TEMP_CRDT_INFO_PARAMS = "peer%d_proxy_params:%s\r\n";
        private static final String TEMP_CRDT_INFO_NO_PARAMS = "peer%d_proxy_params:(null)\r\n";

        public PeerMaster setMeta(RedisMeta meta) {
            this.meta = meta;
            return this;
        }

        String toInfo(int index) {
            StringBuilder sb = new StringBuilder();
            sb.append(
                    String.format(TEMP_CRDT_INFO_BASIC, index, meta.getIp(), index, meta.getPort(), index , meta.getGid())
            );
            if(meta instanceof RedisProxyMeta) {
                RedisProxyMeta proxyMeta = (RedisProxyMeta)meta;
                XpipeRedisProxy proxy = (XpipeRedisProxy)(proxyMeta.getProxy());
                logger.info("index {}", index);
                sb.append(
                        String.format(TEMP_CRDT_INFO_SERVERS,
                                index,
                                index,
                                String.join(",", proxy.getServers().stream().map(endpoint -> {
                            return endpoint.getHost() + ":" + endpoint.getPort();
                        }).collect(Collectors.toList())))
                );
                if(proxy.getParams() != null) {
                    sb.append(
                            String.format(TEMP_CRDT_INFO_PARAMS, index, proxy.getParams())
                    );
                } else {
                    sb.append(
                            String.format(TEMP_CRDT_INFO_NO_PARAMS, index)
                    );
                }
            }
            return sb.toString();
        }

    }
    Map<Long, PeerMaster> peers = new HashMap<>();
    private String mockCRDTInfoResp() {
        StringBuilder sb = new StringBuilder();
        AtomicInteger index = new AtomicInteger(0);
        peers.entrySet().stream().forEach((entry) -> {
            sb.append(entry.getValue().toInfo(index.get()));
            index.incrementAndGet();
        });
        String content = sb.toString();
        return String.format("$%d\r\n%s\r\n", content.length(), content);
    }

    String version = "1.0.14";
    private String mockInfoServerResp() {
        String content = "";
        if (null != version) {
            content = "xredis_crdt_version:" + version;
        }
        return String.format("$%d\r\n%s", content.length(), content);
    }

    List<String> peerofRequest = new LinkedList<>();


    static final String r1 = "$506\r\n" +
            "# Replication\r\n" +
            "role:master\r\n" +
            "connected_slaves:2\r\n" +
            "slave0:ip=10.61.78.89,port=6379,state=online,offset=1698243329929,lag=1\r\n" +
            "slave1:ip=10.60.155.59,port=6406,state=online,offset=1698243332219,lag=1\r\n" +
            "master_replid:6d250cb01cf86c0db8652dfffc478889b69f60d7\r\n" +
            "master_replid2:f0ac9301446323a8006719b9005228a662e5486f\r\n" +
            "master_repl_offset:1698243351505\r\n" +
            "second_repl_offset:1634664143124\r\n" +
            "repl_backlog_active:1\r\n" +
            "repl_backlog_size:536870912\r\n" +
            "repl_backlog_first_byte_offset:1697706480594\r\n" +
            "repl_backlog_histlen:536870912\r\n\r\n";
    static final String r2 = "$506\r\n" +
            "# Replication\r\n" +
            "role:master\r\n" +
            "connected_slaves:2\r\n" +
            "slave0:ip=10.61.78.89,port=6379,state=online,offset=1698243329929,lag=1\r\n" +
            "slave1:ip=10.60.155.59,port=6406,state=online,offset=1698243332219,lag=1\r\n" +
            "master_replid:6d250cb01cf86c0db8652dfffc478889b69f60d7\r\n" +
            "master_replid2:f0ac9301446323a8006719b9005228a662e5486f\r\n" +
            "master_repl_offset:1698243351505\r\n" +
            "second_repl_offset:1634664143124\r\n" +
            "repl_backlog_active:1\r\n" +
            "repl_backlog_size:536870912\r\n" +
            "repl_backlog_first_byte_offset:1697706480594\r\n" +
            "repl_backlog_histlen:53687091212\r\n";
    static final String r3 = "$507\r\n" +
            "# Replication\r\n" +
            "role:master\r\n" +
            "connected_slaves:2\r\n" +
            "slave0:ip=10.61.78.89,port=6379,state=online,offset=1698243329929,lag=1\r\n" +
            "slave1:ip=10.60.155.59,port=6406,state=online,offset=1698243332219,lag=1\r\n" +
            "master_replid:6d250cb01cf86c0db8652dfffc478889b69f60d7\r\n" +
            "master_replid2:f0ac9301446323a8006719b9005228a662e5486f\r\n" +
            "master_repl_offset:1698243351505\r\n" +
            "second_repl_offset:1634664143124\r\n" +
            "repl_backlog_active:1\r\n" +
            "repl_backlog_size:536870912\r\n" +
            "repl_backlog_first_byte_offset:1697706480594\r\n" +
            "repl_backlog_histlen:536870912121\r\n";
    static final String r4 = "$500\r\n" +
            "# Replication\r\n" +
            "role:master\r\n" +
            "connected_slaves:2\r\n" +
            "slave0:ip=10.61.78.89,port=6379,state=online,offset=1698243329929,lag=1\r\n" +
            "slave1:ip=10.60.155.59,port=6406,state=online,offset=1698243332219,lag=1\r\n" +
            "master_replid:6d250cb01cf86c0db8652dfffc478889b69f60d7\r\n" +
            "master_replid2:f0ac9301446323a8006719b9005228a662e5486f\r\n" +
            "master_repl_offset:1698243351505\r\n" +
            "second_repl_offset:1634664143124\r\n" +
            "repl_backlog_active:1\r\n" +
            "repl_backlog_size:536870912\r\n" +
            "repl_backlog_first_byte_offset:1697706480594\r\n" +
            "repl_backlog_histlen:53687\r\n";
    String InfoReplicationResult = r4;
    @Before
    public void before() throws Exception {
        redisServer = startServer(port, new Function<String, String>() {
            @Override
            public String apply(String s) {
                if (s.startsWith("crdt.info")) {
                    return mockCRDTInfoResp();
                } else if (s.startsWith("peerof")) {
                    logger.info("[peeof] {}", s);
                    peerofRequest.add(s.trim());
                } else if (s.startsWith("info server")) {
                    return mockInfoServerResp();
                } else if (s.startsWith("info replication")) {
                    return InfoReplicationResult;
                } else if(s.startsWith("ping")) {
                    return "+PONG\r\n";
                } else if(s.startsWith("publish")) {
                    return ":1\r\n";
                }

                return "+OK\r\n";
            }
        });
    }

    protected String clusterId = "cluster1", shardId = "shard1";
    protected Map<Long, RedisMeta > expectPeerMaster = new HashMap<>();
    private List<RedisMeta> mockUpstreamPeerMaster() {
        List<RedisMeta> upstreamPeerMasters = new ArrayList<>();
        expectPeerMaster.forEach((gid, peerMaster) -> {
            upstreamPeerMasters.add(peerMaster);
        });
        return upstreamPeerMasters;
    }

    @Test
    public void testNoneChange() throws Exception {
        XpipeRedisProxy proxy =  XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30");
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(proxy).setGid(1L).setIp("127.0.0.10").setPort(20));
        PeerMasterAdjustJob peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 1);
        Assert.assertEquals(peerofRequest.get(0), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30");

        // none -> proxy tls peer
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30 PROXYTLS://127.0.0.1:10")).setGid(1L).setIp("127.0.0.10").setPort(20));
         peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 2);
        Assert.assertEquals(peerofRequest.get(1), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30 proxy-params PROXYTLS://127.0.0.1:10");

         // none -> peer
         expectPeerMaster.put(1L, new RedisMeta().setGid(1L).setIp("127.0.0.10").setPort(20));
         peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
         peerMasterAdjustJob.execute().get();
         Assert.assertEquals(peerofRequest.size(), 3);
         Assert.assertEquals(peerofRequest.get(2), "peerof 1 127.0.0.10 20");

         //one peer add  peer
        peers.put(2L, new PeerMaster().setMeta(new RedisMeta().setGid(2L).setIp("127.0.0.40").setPort(20)));
        expectPeerMaster.put(1L, new RedisMeta().setGid(1L).setIp("127.0.0.10").setPort(20));
        expectPeerMaster.put(2L, new RedisMeta().setGid(2L).setIp("127.0.0.40").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 4);
        Assert.assertEquals(peerofRequest.get(3), "peerof 1 127.0.0.10 20");

    }



    //none_proxy peer
    @Test
    public void testPeerChange() throws Exception {
        peers.put(1L, new PeerMaster().setMeta(new RedisMeta().setGid(1L).setIp("127.0.0.10").setPort(20)));
        XpipeRedisProxy proxy =  XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30 PROXYTLS://127.0.0.1:20");
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(proxy).setGid(1L).setIp("127.0.0.10").setPort(20));
        // peer -> proxytls peer
        PeerMasterAdjustJob peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 1);
        Assert.assertEquals(peerofRequest.get(0), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30 proxy-params PROXYTLS://127.0.0.1:20");
        // peer -> proxy peer
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30")).setGid(1L).setIp("127.0.0.10").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 2);
        Assert.assertEquals(peerofRequest.get(1), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30");

         // peer  change host
         expectPeerMaster.put(1L, new RedisMeta().setGid(1L).setIp("127.0.0.1").setPort(20));

         peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 3);
        Assert.assertEquals(peerofRequest.get(2), "peerof 1 127.0.0.1 20");

        // (two peer) peer  change
        expectPeerMaster.put(2L, new RedisMeta().setGid(2L).setIp("127.0.0.1").setPort(120));
        peers.put(2L, new PeerMaster().setMeta(new RedisMeta().setGid(2L).setIp("127.0.0.1").setPort(120)));
        expectPeerMaster.put(1L, new RedisMeta().setGid(1L).setIp("127.0.0.10").setPort(30));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 4);
        Assert.assertEquals(peerofRequest.get(3), "peerof 1 127.0.0.10 30");
        //two peer change one
        expectPeerMaster.put(1L, new RedisMeta().setGid(1L).setIp("127.0.0.10").setPort(30));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 5);
        Assert.assertEquals(peerofRequest.get(4), "peerof 1 127.0.0.10 30");
    }

    //proxy peer change
    @Test
    public void testPPChange() throws Exception {
        peers.put(1L, new PeerMaster().setMeta(new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30")).setGid(1L).setIp("127.0.0.10").setPort(20)));
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30")).setGid(1L).setIp("127.0.0.1").setPort(20));
        //proxy peer change port
        PeerMasterAdjustJob peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 1);
        Assert.assertEquals(peerofRequest.get(0), "peerof 1 127.0.0.1 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30");

        //peer change proxy
         expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:40")).setGid(1L).setIp("127.0.0.10").setPort(20));
         peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 2);
        Assert.assertEquals(peerofRequest.get(1), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:40");

        //peer add tls proxy
        XpipeRedisProxy proxy =  XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30 PROXYTLS://127.0.0.1:20");
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(proxy).setGid(1L).setIp("127.0.0.10").setPort(20));
         peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 3);
        Assert.assertEquals(peerofRequest.get(2), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30 proxy-params PROXYTLS://127.0.0.1:20");

        //proxy peer -> peer
        expectPeerMaster.put(1L, new RedisMeta().setGid(1L).setIp("127.0.0.1").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 4);
        Assert.assertEquals(peerofRequest.get(3), "peerof 1 127.0.0.1 20");

        //proxy peer change host
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30")).setGid(1L).setIp("127.0.0.1").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 5);
        Assert.assertEquals(peerofRequest.get(4), "peerof 1 127.0.0.1 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30");

        //add proxy
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30,PROXYTCP://127.0.0.1:130")).setGid(1L).setIp("127.0.0.1").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 6);
        Assert.assertEquals(peerofRequest.get(5), "peerof 1 127.0.0.1 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30,127.0.0.1:130");

        //change proxy
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:130")).setGid(1L).setIp("127.0.0.1").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 7);
        Assert.assertEquals(peerofRequest.get(6), "peerof 1 127.0.0.1 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:130");

        //remove change
        peers.put(1L, new PeerMaster().setMeta(new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30,PROXYTCP://127.0.0.1:130")).setGid(1L).setIp("127.0.0.1").setPort(20)));
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:130")).setGid(1L).setIp("127.0.0.1").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 8);
        Assert.assertEquals(peerofRequest.get(7), "peerof 1 127.0.0.1 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:130");
    }

    @Test
    public void testPTPChange() throws Exception {
        peers.put(1L, new PeerMaster().setMeta(new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30 PROXYTLS://127.0.0.1:40")).setGid(1L).setIp("127.0.0.10").setPort(20)));
        expectPeerMaster.put(1L, new RedisMeta().setGid(1L).setIp("127.0.0.10").setPort(20));
        //proxy tls peer -> non_proxy peer
        Assert.assertEquals(peerofRequest.size(), 0);
        PeerMasterAdjustJob peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 1);
        Assert.assertEquals(peerofRequest.get(0), "peerof 1 127.0.0.10 20");

        //proxy tls peer -> proxy peer
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30")).setGid(1L).setIp("127.0.0.10").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
        Pair.of("127.0.0.1", redisServer.getPort()), false,
        getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 2);
        Assert.assertEquals(peerofRequest.get(1), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30");

        //proxy tls peer -> change host
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30 PROXYTLS://127.0.0.1:40")).setGid(1L).setIp("127.0.0.1").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 3);
        Assert.assertEquals(peerofRequest.get(2), "peerof 1 127.0.0.1 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30 proxy-params PROXYTLS://127.0.0.1:40");

        //proxy tls peer -> change port
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30 PROXYTLS://127.0.0.1:40")).setGid(1L).setIp("127.0.0.10").setPort(120));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 4);
        Assert.assertEquals(peerofRequest.get(3), "peerof 1 127.0.0.10 120 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30 proxy-params PROXYTLS://127.0.0.1:40");

        //add peer
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30,PROXYTCP://127.0.0.1:130 PROXYTLS://127.0.0.1:40")).setGid(1L).setIp("127.0.0.10").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 5);
        Assert.assertEquals(peerofRequest.get(4), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30,127.0.0.1:130 proxy-params PROXYTLS://127.0.0.1:40");

        //add tls
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30 PROXYTLS://127.0.0.1:40,PROXYTLS://127.0.0.1:140")).setGid(1L).setIp("127.0.0.10").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 6);
        Assert.assertEquals(peerofRequest.get(5), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30 proxy-params PROXYTLS://127.0.0.1:40,PROXYTLS://127.0.0.1:140");

        //change tls
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30 PROXYTLS://127.0.0.1:140")).setGid(1L).setIp("127.0.0.10").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 7);
        Assert.assertEquals(peerofRequest.get(6), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30 proxy-params PROXYTLS://127.0.0.1:140");

        //remove tls
        peers.put(1L, new PeerMaster().setMeta(new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30 PROXYTLS://127.0.0.1:40,PROXYTLS://127.0.0.1:140")).setGid(1L).setIp("127.0.0.10").setPort(20)));
        expectPeerMaster.put(1L, new RedisProxyMeta().setProxy(XpipeRedisProxy.read("PROXYTCP://127.0.0.1:30 PROXYTLS://127.0.0.1:140")).setGid(1L).setIp("127.0.0.10").setPort(20));
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(peerofRequest.size(), 8);
        Assert.assertEquals(peerofRequest.get(7), "peerof 1 127.0.0.10 20 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:30 proxy-params PROXYTLS://127.0.0.1:140");

    }


}
