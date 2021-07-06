package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl.XpipeRedisProxy;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.protocols.DefaultProxyConnectProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CrdtInfoExtractorPeerTest {
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
        List<RedisMeta> metas = re.extractPeerMasters();
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
        List<RedisMeta> metas = re.extractPeerMasters();
        Assert.assertEquals(metas.size() , 1);
        RedisMeta meta = metas.get(0);
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
                "peer0_proxy_params:(null)\n" +
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
        List<RedisMeta> metas = re.extractPeerMasters();
        Assert.assertEquals(metas.size() , 1);
        RedisMeta meta = metas.get(0);
        Assert.assertEquals(meta.getIp(), "127.0.0.1");
        Assert.assertEquals(meta.getPort().intValue(), 0);
        XpipeRedisProxy proxy = XpipeRedisProxy.read("PROXYTCP://127.0.0.1:1,PROXYTCP://127.0.0.1:2");
        Assert.assertEquals(proxy, ((RedisProxyMeta)meta).getProxy());
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
                "peer0_proxy_params:PROXYTLS://127.0.0.1:10,PROXYTLS://127.0.0.1:11\n" +
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
        List<RedisMeta> metas = re.extractPeerMasters();
        Assert.assertEquals(metas.size() , 1);
        RedisMeta meta = metas.get(0);
        Assert.assertEquals(meta.getIp(), "127.0.0.1");
        Assert.assertEquals(meta.getPort().intValue(), 0);
        XpipeRedisProxy proxy = XpipeRedisProxy.read("PROXYTCP://127.0.0.1:1,PROXYTCP://127.0.0.1:2 PROXYTLS://127.0.0.1:10,PROXYTLS://127.0.0.1:11");
        Assert.assertEquals(proxy, ((RedisProxyMeta)meta).getProxy());

    }
}
