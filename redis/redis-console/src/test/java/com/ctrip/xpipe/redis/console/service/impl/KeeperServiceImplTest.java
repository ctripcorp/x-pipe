package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;

import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Jan 23, 2018
 */
public class KeeperServiceImplTest {

    @Mock
    MetaCache metaCache;

    @InjectMocks
    KeeperServiceImpl keeperService;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIsKeeper() throws Exception {
        when(metaCache.allKeepers()).thenReturn(Collections.singleton(new HostPort("127.0.0.1", 6379)));
        HostPort hostPort = new HostPort("127.0.0.1", 6379);
        Assert.assertTrue(keeperService.isKeeper(hostPort));
        Assert.assertFalse(keeperService.isKeeper(new HostPort("128.0.0.1", 6380)));
    }

}