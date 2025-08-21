package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.KeeperStateModel;
import com.ctrip.xpipe.redis.console.healthcheck.session.KeeperConsoleSessionManager;
import com.ctrip.xpipe.redis.console.service.impl.KeeperSessionServiceImpl;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.client.RestOperations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

@RunWith(MockitoJUnitRunner.class)
public class KeeperSessionServiceTest {

    @Mock
    private KeeperConsoleSessionManager sessionManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private RestOperations restTemplate;

    @InjectMocks
    private KeeperSessionServiceImpl keeperSessionService;

    public static final String DC = "jq";

    public static final String CLUSTER = "cluster";

    public static final String SHARD = "shard";

    public static final String IP = "127.0.0.1";

    public static final int PORT = 6379;

    @Test
    public void testInfoKeeper() {
        RedisSession session = Mockito.mock(RedisSession.class);
        CommandFuture future = Mockito.mock(CommandFuture.class);
        Callbackable<String> callback = new Callbackable<String>() {
            @Override
            public void success(String message) {

            }

            @Override
            public void fail(Throwable throwable) {

            }
        };
        Mockito.when(sessionManager.findOrCreateSession(new DefaultEndPoint(IP, PORT))).thenReturn(session);
        Mockito.when(session.info(InfoCommand.INFO_TYPE.REPLICATION.cmd(), callback)).thenReturn(future);
        CommandFuture<String> future1 = keeperSessionService.infoKeeper(IP, PORT, InfoCommand.INFO_TYPE.REPLICATION.cmd(), callback);
        Assert.assertEquals(future1, future);
    }

    @Test
    public void testKeeperRole() {
        RedisSession session = Mockito.mock(RedisSession.class);
        CommandFuture future = Mockito.mock(CommandFuture.class);
        RedisSession.RollCallback callback = new RedisSession.RollCallback() {
            @Override
            public void role(String role, Role detail) {

            }

            @Override
            public void fail(Throwable e) {

            }
        };
        Mockito.when(sessionManager.findOrCreateSession(new DefaultEndPoint(IP, PORT))).thenReturn(session);
        Mockito.when(session.role(callback)).thenReturn(future);
        CommandFuture<Role> redisRole = keeperSessionService.getKeeperRole(IP, PORT, callback);
        Assert.assertEquals(redisRole, future);
    }

    @Test
    public void testGetKeeperReplId() {
        long replId = 123L;
        KeeperInstanceMeta keeperInstanceMeta = new KeeperInstanceMeta();
        keeperInstanceMeta.setReplId(replId);
        String url = "http://" + IP + ":8080/keepers/port/{port}";;
        Mockito.when(restTemplate.getForObject(url, KeeperInstanceMeta.class, PORT)).thenReturn(keeperInstanceMeta);
        KeeperInstanceMeta keeperReplId = keeperSessionService.getKeeperReplId(IP, PORT);
        Assert.assertEquals(Long.parseLong(String.valueOf(keeperReplId.getReplId())), replId);
    }

    @Test
    public void testGetShardAllKeeperState() throws Exception {
        List<KeeperMeta> keeperMetas = new ArrayList<>();
        KeeperMeta keeperMeta = Mockito.mock(KeeperMeta.class);
        keeperMetas.add(keeperMeta);
        Mockito.when(keeperMeta.getIp()).thenReturn(IP);
        Mockito.when(keeperMeta.getPort()).thenReturn(PORT);
        Mockito.when(metaCache.getKeeperOfDcClusterShard(DC, CLUSTER, SHARD)).thenReturn(keeperMetas);
        RedisSession session = Mockito.mock(RedisSession.class);
        Mockito.when(sessionManager.findOrCreateSession(new DefaultEndPoint(IP, PORT))).thenReturn(session);
        CommandFuture futureRole = Mockito.mock(CommandFuture.class);
        CommandFuture futureInfo = Mockito.mock(CommandFuture.class);
        Mockito.when(session.role(any())).thenReturn(futureRole);
        Mockito.when(session.info(anyString(), any())).thenReturn(futureInfo);
        long replId = 123L;
        KeeperInstanceMeta keeperInstanceMeta = new KeeperInstanceMeta();
        keeperInstanceMeta.setReplId(replId);
        String url = "http://" + IP + ":8080/keepers/port/{port}";;
        Mockito.when(restTemplate.getForObject(url, KeeperInstanceMeta.class, PORT)).thenReturn(keeperInstanceMeta);
        InterruptedException exception = Mockito.mock(InterruptedException.class);
        Mockito.when(futureRole.get()).thenThrow(exception);
        Mockito.when(futureInfo.get()).thenThrow(exception);
        List<KeeperStateModel> shardAllKeeperState = keeperSessionService.getShardAllKeeperState(DC, CLUSTER, SHARD);
        Assert.assertEquals(shardAllKeeperState.size(), 1);
        Assert.assertEquals(Long.parseLong(String.valueOf(shardAllKeeperState.get(0).getReplId())), replId);
        Assert.assertEquals(shardAllKeeperState.get(0).getHost(), IP);
        Assert.assertEquals(shardAllKeeperState.get(0).getPort(), PORT);
        Map<String, Throwable> map = new ConcurrentHashMap<>();
        map.put("commandFutureExecute", exception);
        Assert.assertEquals(shardAllKeeperState.get(0).getErrs(), map);
    }

    @Test
    public void testKeeperInfo() {
        String infoResult = "# Replication\n" +
                "role:slave\n" +
                "keeperrole:keeper\n" +
                "state:ACTIVE\n" +
                "master_host:10.118.123.56\n" +
                "master_port:20580\n" +
                "master_link_status:up\n" +
                "slave_repl_offset:1920376830\n" +
                "slave_priority:0\n" +
                "connected_slaves:2\n" +
                "slave0:ip=10.118.79.45,port=6670,state=online,offset=1920376558,lag=0,remotePort=44506\n" +
                "slave1:ip=10.118.79.251,port=6670,state=online,offset=1920376558,lag=0,remotePort=33668\n" +
                "master_replid:416c848e9c33054c0ae0a9ec9012444a8e0a9152\n" +
                "master_replid2:67df6cf4f337cf9d9c277efaf7bd2f2910c4409c\n" +
                "master_repl_offset:1920376830\n" +
                "second_repl_offset:1878160715\n" +
                "repl_backlog_active:1\n" +
                "repl_backlog_first_byte_offset:1877685788\n" +
                "master_repl_offset:1920376830\n" +
                "repl_backlog_size:42691043\n" +
                "repl_backlog_histlen:42691043";
        KeeperStateModel model = new KeeperStateModel();
        InfoResultExtractor extractor = new InfoResultExtractor(infoResult);
        model.setState(extractor.getKeeperState())
                .setMasterHost(extractor.getKeyKeeperMasterHost())
                .setMasterPort(extractor.getKeyKeeperMasterPort())
                .setMasterReplOffset(extractor.getMasterReplOffset())
                .setReplBacklogSize(extractor.getKeyKeeperReplBacklogSize())
                .setSlavesMap(extractor.getKeyKeeperSlaves());
        Assert.assertEquals(model.getSlaves().size(), 2);
        Assert.assertEquals(model.getSlaves().get(0).getHost(), "10.118.79.45");
        Assert.assertEquals(model.getSlaves().get(0).getPort(), 6670);
        Assert.assertEquals(model.getSlaves().get(0).getState(), "online");
        Assert.assertEquals(model.getSlaves().get(0).getOffset(), 1920376558);
        Assert.assertEquals(model.getSlaves().get(0).getRemotePort(), 44506);
    }

}
