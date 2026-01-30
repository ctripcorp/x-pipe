package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.netty.filechannel.DefaultReferenceFileRegion;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileChannel;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.ratelimit.ReplDelayConfig;
import com.ctrip.xpipe.utils.OffsetNotifier;
import io.netty.channel.FileRegion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OffsetCommandReaderTest extends AbstractTest {

    OffsetCommandReader reader;

    @Mock
    private CommandFile file;

    @Mock
    private CommandStore store;

    @Mock
    private OffsetNotifier notifier;

    @Mock
    private ReplDelayConfig config;

    @Mock
    private ReferenceFileChannel fileChannel;

    @Before
    public void setupOffsetCommandReaderTest() throws Exception {
        Mockito.when(fileChannel.hasAnythingToRead()).thenReturn(true);

        Mockito.doAnswer(invocationOnMock -> {
            long maxBytes = invocationOnMock.getArgument(0);
            DefaultReferenceFileRegion fileRegion = Mockito.mock(DefaultReferenceFileRegion.class);
            Mockito.when(fileRegion.count()).thenReturn(maxBytes > 0 ? maxBytes : 1024);
            return fileRegion;
        }).when(fileChannel).read(Mockito.anyLong());
    }

    @Test
    public void testReadToWall() throws Exception {
        reader = new OffsetCommandReader(file, 1, 101, 1, store, notifier, config, 1024);
        reader.setFileChannel(fileChannel);

        ReferenceFileRegion region = reader.doRead(10);
        Mockito.verify(fileChannel).read(100);
        Assert.assertEquals(100, region.count());

        region = reader.doRead(10);
        Assert.assertEquals(ReferenceFileRegion.EOF, region);
    }

}
