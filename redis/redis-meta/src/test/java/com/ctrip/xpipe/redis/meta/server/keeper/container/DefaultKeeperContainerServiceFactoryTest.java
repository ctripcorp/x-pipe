package com.ctrip.xpipe.redis.meta.server.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultKeeperContainerServiceFactoryTest {
    private DefaultKeeperContainerServiceFactory keeperContainerServiceFactory;

    @Before
    public void setUp() throws Exception {
        keeperContainerServiceFactory = new DefaultKeeperContainerServiceFactory();
    }

    @Test
    public void getOrCreateKeeperContainerServiceMultipleTimes() throws Exception {
        KeeperContainerMeta someKeeperContainerMeta = mock(KeeperContainerMeta.class);

        KeeperContainerService someService = keeperContainerServiceFactory.getOrCreateKeeperContainerService
                (someKeeperContainerMeta);
        KeeperContainerService anotherService = keeperContainerServiceFactory.getOrCreateKeeperContainerService
                (someKeeperContainerMeta);

        assertEquals(someService, anotherService);
    }

    @Test
    public void getOrCreateKeeperContainerServiceMultipleTimesWithDifferentMeta() throws Exception {
        KeeperContainerMeta someKeeperContainerMeta = mock(KeeperContainerMeta.class);
        KeeperContainerMeta anotherKeeperContainerMeta = mock(KeeperContainerMeta.class);

        KeeperContainerService someService = keeperContainerServiceFactory.getOrCreateKeeperContainerService
                (someKeeperContainerMeta);
        KeeperContainerService anotherService = keeperContainerServiceFactory.getOrCreateKeeperContainerService
                (anotherKeeperContainerMeta);

        assertNotEquals(someService, anotherService);
    }
}