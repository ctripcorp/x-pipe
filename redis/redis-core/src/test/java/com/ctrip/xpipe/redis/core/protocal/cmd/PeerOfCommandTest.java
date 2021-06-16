package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.ProxyRedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl.XpipeRedisProxy;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Test;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class PeerOfCommandTest extends AbstractRedisTest {



    public String convertByteBufToString(ByteBuf buf) {
        String str;
        if(buf.hasArray()) { // 处理堆缓冲区
            str = new String(buf.array(), buf.arrayOffset() + buf.readerIndex(), buf.readableBytes());
        } else { // 处理直接缓冲区以及复合缓冲区
            byte[] bytes = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), bytes);
            str = new String(bytes, 0, buf.readableBytes());
        }
        return str;
    }

    @Test
    public void testPeerOf() throws Exception {
        int port = randomPort();
        PeerOfCommand command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port))
                ,1,
                "127.0.0.1",
                0,
                scheduled);

        Assert.assertEquals(convertByteBufToString(command.getRequest()), "peerof 1 127.0.0.1 0\r\n");

    }

    @Test
    public void testPeerOfWithProxy() throws Exception {

        XpipeRedisProxy proxy = XpipeRedisProxy.read("PROXYTCP://127.0.0.1:1");
        int port = randomPort();
        PeerOfCommand command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port))
                ,1,
                "127.0.0.1",
                0,
                proxy,
                scheduled);



        Assert.assertEquals(convertByteBufToString(command.getRequest()), "peerof 1 127.0.0.1 0 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:1\r\n");

    }

    @Test
    public void testpeerofWithProxyTLS() throws Exception {
        XpipeRedisProxy proxy = XpipeRedisProxy.read("PROXYTCP://127.0.0.1:1 PROXYTLS://127.0.0.1:10");
        int port = randomPort();
        PeerOfCommand command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port
                ))
                ,1,
                "127.0.0.1",
                0,
                proxy,
                scheduled);


        logger.info(convertByteBufToString(command.getRequest()));
        Assert.assertEquals(convertByteBufToString(command.getRequest()), "peerof 1 127.0.0.1 0 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:1 proxy-proxytls PROXYTLS://127.0.0.1:10\r\n");
    }
    @Test
    public void RedisCrdtInfo() throws Exception {
        String result = "# CRDT Replication\n" +
                "ovc:1:0\n" +
                "gcvc:1:0\n" +
                "gid:1\n" +
                "backstreaming:0\n" +
                "connected_slaves:0\n" +
                "master_replid:ba5fb33eee0aba7ea4cb484b4cd93c8cc149eecc\n" +
                "master_replid2:0000000000000000000000000000000000000000\n" +
                "master_repl_offset:0\n" +
                "second_repl_offset:-1\n" +
                "repl_backlog_active:0\n" +
                "repl_backlog_size:134217728\n" +
                "repl_backlog_first_byte_offset:0\n" +
                "repl_backlog_histlen:0\r\n";
        CRDTInfoResultExtractor re = new CRDTInfoResultExtractor(result);
        List<ProxyRedisMeta> metas = re.extractPeerMasters();
        Assert.assertEquals(metas.size() , 0);



    }

    @Test
    public void RedisCrdtInfo2() {
        String result = "# CRDT Replication\n" +
                "ovc:1:0\n" +
                "gcvc:1:0\n" +
                "gid:1\n" +
                "backstreaming:0\n" +
                "#Peer_Master_0\n" +
                "peer0_host:127.0.0.1\n" +
                "peer0_port:0\n" +
                "peer0_gid:2\n" +
                "peer0_dbid:0\n" +
                "peer0_link_status:down\n" +
                "peer0_last_io_seconds_ago:-1\n" +
                "peer0_sync_in_progress:0\n" +
                "peer0_repl_offset:0\n" +
                "peer0_replid:000000\n" +
                "peer0_link_down_since_seconds:1623728404\n" +
                "connected_slaves:0\n" +
                "master_replid:ba5fb33eee0aba7ea4cb484b4cd93c8cc149eecc\n" +
                "master_replid2:0000000000000000000000000000000000000000\n" +
                "master_repl_offset:0\n" +
                "second_repl_offset:-1\n" +
                "repl_backlog_active:0\n" +
                "repl_backlog_size:134217728\n" +
                "repl_backlog_first_byte_offset:0\n" +
                "repl_backlog_histlen:0";
        CRDTInfoResultExtractor re = new CRDTInfoResultExtractor(result);
        List<ProxyRedisMeta> metas = re.extractPeerMasters();
        Assert.assertEquals(metas.size() , 1);
        ProxyRedisMeta meta = metas.get(0);
        Assert.assertEquals(meta.getProxy() , null);
        Assert.assertEquals(meta.getIp(), "127.0.0.1");
        Assert.assertEquals(meta.getPort().intValue(), 0);

    }

    @Test
    public void RedisCrdtInfo3() {
        String result = "# CRDT Replication\n" +
                "ovc:1:0\n" +
                "gcvc:1:0\n" +
                "gid:1\n" +
                "backstreaming:0\n" +
                "#Peer_Master_0\n" +
                "peer0_host:127.0.0.1\n" +
                "peer0_port:0\n" +
                "peer0_gid:2\n" +
                "peer0_dbid:0\n" +
                "peer0_link_status:down\n" +
                "peer0_last_io_seconds_ago:-1\n" +
                "peer0_sync_in_progress:0\n" +
                "peer0_repl_offset:0\n" +
                "peer0_replid:000000\n" +
                "peer0_proxy_type:xpipe_proxy\n" +
                "peer0_proxy_servers:127.0.0.1:1,127.0.0.1:2\n" +
                "peer0_proxy_server:127.0.0.1:1\n" +
                "peer0_link_down_since_seconds:1623729193\n" +
                "connected_slaves:0\n" +
                "master_replid:ba5fb33eee0aba7ea4cb484b4cd93c8cc149eecc\n" +
                "master_replid2:0000000000000000000000000000000000000000\n" +
                "master_repl_offset:0\n" +
                "second_repl_offset:-1\n" +
                "repl_backlog_active:0\n" +
                "repl_backlog_size:134217728\n" +
                "repl_backlog_first_byte_offset:0\n" +
                "repl_backlog_histlen:0";
        CRDTInfoResultExtractor re = new CRDTInfoResultExtractor(result);
        List<ProxyRedisMeta> metas = re.extractPeerMasters();
        Assert.assertEquals(metas.size() , 1);
        ProxyRedisMeta meta = metas.get(0);
        Assert.assertEquals(meta.getIp(), "127.0.0.1");
        Assert.assertEquals(meta.getPort().intValue(), 0);
        XpipeRedisProxy proxy = XpipeRedisProxy.read("PROXYTCP://127.0.0.1:1,PROXYTCP://127.0.0.1:2");
        Assert.assertEquals(proxy, meta.getProxy());
    }

    @Test
    public void RedisCrdtInfo4() {
        String result = "# CRDT Replication\n" +
                "ovc:1:0\n" +
                "gcvc:1:0\n" +
                "gid:1\n" +
                "backstreaming:0\n" +
                "#Peer_Master_0\n" +
                "peer0_host:127.0.0.1\n" +
                "peer0_port:0\n" +
                "peer0_gid:2\n" +
                "peer0_dbid:0\n" +
                "peer0_link_status:down\n" +
                "peer0_last_io_seconds_ago:-1\n" +
                "peer0_sync_in_progress:0\n" +
                "peer0_repl_offset:0\n" +
                "peer0_replid:000000\n" +
                "peer0_proxy_type:xpipe_proxy\n" +
                "peer0_proxy_servers:127.0.0.1:1,127.0.0.1:2\n" +
                "peer0_proxy_server:127.0.0.1:1\n" +
                "peer0_proxy_tls:PROXYTLS://127.0.0.1:10,PROXYTLS://127.0.0.1:11\n" +
                "peer0_link_down_since_seconds:1623729193\n" +
                "connected_slaves:0\n" +
                "master_replid:ba5fb33eee0aba7ea4cb484b4cd93c8cc149eecc\n" +
                "master_replid2:0000000000000000000000000000000000000000\n" +
                "master_repl_offset:0\n" +
                "second_repl_offset:-1\n" +
                "repl_backlog_active:0\n" +
                "repl_backlog_size:134217728\n" +
                "repl_backlog_first_byte_offset:0\n" +
                "repl_backlog_histlen:0";
        CRDTInfoResultExtractor re = new CRDTInfoResultExtractor(result);
        List<ProxyRedisMeta> metas = re.extractPeerMasters();
        Assert.assertEquals(metas.size() , 1);
        ProxyRedisMeta meta = metas.get(0);
        Assert.assertEquals(meta.getIp(), "127.0.0.1");
        Assert.assertEquals(meta.getPort().intValue(), 0);
        XpipeRedisProxy proxy = XpipeRedisProxy.read("PROXYTCP://127.0.0.1:1,PROXYTCP://127.0.0.1:2 PROXYTLS://127.0.0.1:10,PROXYTLS://127.0.0.1:11");
        Assert.assertEquals(proxy, meta.getProxy());
    }
}
