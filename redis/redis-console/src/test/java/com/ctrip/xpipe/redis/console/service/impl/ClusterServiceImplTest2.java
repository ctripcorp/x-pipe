package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 20, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterServiceImplTest2 extends AbstractServiceImplTest{

    @InjectMocks
    private ClusterServiceImpl clusterService;

    @Mock
    private RouteServiceImpl routeService;

    @Mock
    private ProxyServiceImpl proxyService;

    @Mock
    private DcServiceImpl dcService;

    @Mock
    private ClusterDao clusterDao;

    @Mock
    private MetaCache metaCache;

    @Before
    public void beforeTest() {
        when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
    }

    @Test
    public void testFindUnmatchedClusterRoutes() {
        clusterService.findUnmatchedClusterRoutes();
    }

    private XpipeMeta mockXpipeMeta() {
        XpipeMeta meta = new XpipeMeta();

//        for (String dc: mockDcs) {
//            meta.addDc(mockDcMeta(dc));
//        }

        return meta;
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/cluster-service-impl-test2.sql");
    }

}
