package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.LogicalBuOrgTbl;
import com.ctrip.xpipe.redis.console.model.LogicalBuOrgTblDao;
import com.ctrip.xpipe.redis.console.model.LogicalBuTbl;
import com.ctrip.xpipe.redis.console.query.DalQueryHandler;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LogicalBuServiceImplResolveTest {

    @Spy
    @InjectMocks
    private LogicalBuServiceImpl logicalBuService;

    @Mock
    private DalQueryHandler queryHandler;

    @Before
    public void setUp() {
        ReflectionTestUtils.setField(logicalBuService, "queryHandler", queryHandler);
    }

    @Test
    public void testResolveReturnsZeroWhenOrgUnbound() {
        Assert.assertEquals(0L, logicalBuService.resolveLogicalBuIdForCluster("cluster1", 0L));
    }

    @Test
    public void testResolveReturnsZeroWhenClusterNameEmpty() {
        Assert.assertEquals(0L, logicalBuService.resolveLogicalBuIdForCluster("", 100L));
    }

    @Test
    public void testResolveReturnsZeroWhenNoCandidate() {
        when(queryHandler.handleQuery(any())).thenReturn(Lists.newArrayList());
        Assert.assertEquals(0L, logicalBuService.resolveLogicalBuIdForCluster("cluster1", 100L));
    }

    @Test
    public void testResolveHashModuloDeterministic() {
        LogicalBuTbl bu1 = new LogicalBuTbl().setId(10L);
        LogicalBuTbl bu2 = new LogicalBuTbl().setId(20L);
        when(queryHandler.handleQuery(any())).thenReturn(Lists.newArrayList(bu1, bu2));

        String clusterName = "test-cluster";
        int idx = Math.floorMod(clusterName.hashCode(), 2);
        long expected = idx == 0 ? 10L : 20L;
        Assert.assertEquals(expected, logicalBuService.resolveLogicalBuIdForCluster(clusterName, 100L));
    }

    @Test
    public void testSoftDeleteOrgMappingsAllowsZeroRows() {
        LogicalBuOrgTblDao orgDao = mock(LogicalBuOrgTblDao.class);
        when(orgDao.createLocal()).thenReturn(new LogicalBuOrgTbl());
        ReflectionTestUtils.setField(logicalBuService, "logicalBuOrgTblDao", orgDao);

        ReflectionTestUtils.invokeMethod(logicalBuService, "softDeleteOrgMappings", 1L);

        verify(queryHandler).handleDelete(any(), eq(true));
    }
}
