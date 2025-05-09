package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class FreshRdbOnlyPsyncTest extends AbstractRedisTest {

    @Mock
    private ReplicationStore replicationStore;

    @Mock
    private MetaStore metaStore;

    @Before
    public void beforeDefaultPsyncTest() throws Exception{
        when(replicationStore.getMetaStore()).thenReturn(metaStore);
        when(replicationStore.getEndOffset()).thenReturn(-1L);

    }

    @Test
    public void testFreshRdbOnlyPsync() throws Exception {
        String replId = RunidGenerator.DEFAULT.generateRunid();
        int offset = 100;
        Server redisServer = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                logger.info("[testFreshRdbOnlyPsync] {}", s);
                if (s.trim().equalsIgnoreCase("psync ? -3")) {
                    return String.format("+FULLRESYNC %s %d\r\n", replId, offset);
                } else {
                    return "+OK\r\n";
                }
            }
        });
        Endpoint redisEndpoint = new DefaultEndPoint("127.0.0.1", redisServer.getPort());
        FreshRdbOnlyGapAllowedSync gasync = new FreshRdbOnlyGapAllowedSync(NettyPoolUtil.createNettyPool(redisEndpoint), replicationStore, scheduled);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger masetrOffset = new AtomicInteger(0);
        gasync.addPsyncObserver(new PsyncObserver() {
            @Override
            public void onFullSync(long masterRdbOffset) {
                masetrOffset.set((int)masterRdbOffset);
                latch.countDown();
            }
            @Override
            public void reFullSync() {
            }
            @Override
            public void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) throws IOException {
            }
            @Override
            public void endWriteRdb() {
            }
            @Override
            public void onContinue(String requestReplId, String responseReplId) {
            }
            @Override
            public void onKeeperContinue(String replId, long beginOffset) {
            }
            @Override
            public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {
            }
        });
        gasync.execute().addListener(new CommandFutureListener<Object>() {

            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()){
                    logger.error("[operationComplete]", commandFuture.cause());
                }
            }
        });

        latch.await(1000, TimeUnit.SECONDS);
        Assert.assertEquals(offset, masetrOffset.get());
    }

}
