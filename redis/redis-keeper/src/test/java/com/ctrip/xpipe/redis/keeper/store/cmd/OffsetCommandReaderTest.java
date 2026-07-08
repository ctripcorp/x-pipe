package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ratelimit.ReplDelayConfig;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.withSettings;

@RunWith(MockitoJUnitRunner.class)
public class OffsetCommandReaderTest {

    OffsetCommandReader reader;

    private CommandStore commandStore;

    private AsyncCommandStore asyncCommandStore;

    @Mock
    private AsyncFileSystem asyncFileSystem;

    @Mock
    private AsyncSegmentFile readAsyncSegmentFile;

    @Mock
    private OffsetNotifier notifier;

    @Mock
    private ReplDelayConfig config;

    @Before
    public void setupOffsetCommandReaderTest() throws Exception {
        commandStore = Mockito.mock(CommandStore.class,
                withSettings().extraInterfaces(AsyncCommandStore.class));
        asyncCommandStore = (AsyncCommandStore) commandStore;

        Mockito.when(asyncCommandStore.getAsyncFileSystem()).thenReturn(asyncFileSystem);
        Mockito.when(asyncCommandStore.getCommandBaseDir()).thenReturn(new java.io.File("/tmp"));
        Mockito.when(asyncCommandStore.getCommandFileNamePrefix()).thenReturn("cmd_");
        Mockito.when(asyncCommandStore.getCommandIndexPrefixes()).thenReturn(Collections.emptyList());
        Mockito.when(asyncCommandStore.getFileSystemReplId()).thenReturn(ReplId.from(1L));
        Mockito.when(asyncFileSystem.open(Mockito.anyString(), Mockito.anyString(), Mockito.anyList(), Mockito.eq(false), Mockito.eq("repl_1")))
                .thenReturn(CompletableFuture.completedFuture(readAsyncSegmentFile));
        Mockito.when(commandStore.totalLength()).thenReturn(200L);
        Mockito.when(config.getPsyncLimitPerSecond()).thenReturn(-1);
    }

    @Test
    public void testReadToWall() throws Exception {
        reader = new OffsetCommandReader(1, 101, commandStore, notifier, config, 1024);

        ReferenceFileRegion region = reader.doRead(10);
        Assert.assertEquals(100, region.count());

        region = reader.doRead(10);
        Assert.assertEquals(ReferenceFileRegion.EOF, region);
    }

    @Test
    public void testCloseReleasesReadSegment() throws Exception {
        reader = new OffsetCommandReader(1, 101, commandStore, notifier, config, 1024);
        Mockito.when(asyncFileSystem.close(readAsyncSegmentFile))
                .thenReturn(CompletableFuture.completedFuture(null));

        reader.close();

        Mockito.verify(asyncFileSystem).close(readAsyncSegmentFile);
        Mockito.verify(commandStore).removeReader(reader);
    }

}
