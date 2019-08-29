package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DefaultMetaCacheTest {

    @Mock
    private DcMetaService dcMetaService;

    @Mock
    private DcService dcService;

    @Mock
    private ConsoleConfig consoleConfig;

    @InjectMocks
    private DefaultMetaCache metaCache = new DefaultMetaCache();

    @Before
    public void beforeDefaultMetaCacheTest() {
        MockitoAnnotations.initMocks(this);
    }


    @Test
    public void getRouteIfPossible() {
        HostPort hostPort = new HostPort("127.0.0.1", 6379);
        XpipeMetaManager xpipeMetaManager = mock(XpipeMetaManager.class);
        when(xpipeMetaManager.findMetaDesc(hostPort)).thenReturn(null);
        metaCache.setMeta(new Pair<>(mock(XpipeMeta.class), xpipeMetaManager));
        metaCache.getRouteIfPossible(hostPort);
    }
}