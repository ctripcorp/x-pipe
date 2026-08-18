package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.model.LogicalBuOrgTbl;
import com.ctrip.xpipe.redis.console.model.LogicalBuOrgTblDao;
import com.ctrip.xpipe.redis.console.model.LogicalBuTbl;
import com.ctrip.xpipe.redis.console.model.LogicalBuTblDao;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.query.DalQueryHandler;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.unidal.dal.jdbc.DalException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

    @Test
    public void testReplaceOrgMappingsUsesInsertBatch() throws DalException {
        LogicalBuOrgTblDao orgDao = mock(LogicalBuOrgTblDao.class);
        when(orgDao.createLocal()).thenAnswer(invocation -> new LogicalBuOrgTbl());
        when(orgDao.insertBatch(any(LogicalBuOrgTbl[].class))).thenReturn(new int[]{1, 1});
        doAnswer(invocation -> {
            DalQuery<?> query = invocation.getArgument(0);
            query.doQuery();
            return null;
        }).when(queryHandler).handleInsert(any());
        ReflectionTestUtils.setField(logicalBuService, "logicalBuOrgTblDao", orgDao);

        ReflectionTestUtils.invokeMethod(logicalBuService, "replaceOrgMappings", 7L, Lists.newArrayList(11L, 22L));

        verify(queryHandler).handleDelete(any(), eq(true));
        ArgumentCaptor<LogicalBuOrgTbl[]> captor = ArgumentCaptor.forClass(LogicalBuOrgTbl[].class);
        verify(orgDao).insertBatch(captor.capture());
        LogicalBuOrgTbl[] inserted = captor.getValue();
        Assert.assertEquals(2, inserted.length);
        Assert.assertEquals(7L, inserted[0].getLogicalBuId());
        Assert.assertEquals(11L, inserted[0].getCmsOrgId());
        Assert.assertEquals(22L, inserted[1].getCmsOrgId());
        Assert.assertNotNull(inserted[0].getDataChangeLastTime());
        Assert.assertNotNull(inserted[1].getDataChangeLastTime());
    }

    @Test
    public void testCreateSetsDataChangeLastTime() throws DalException {
        LogicalBuTblDao buDao = mock(LogicalBuTblDao.class);
        LogicalBuTbl proto = new LogicalBuTbl();
        when(buDao.createLocal()).thenReturn(proto);
        when(buDao.insert(any(LogicalBuTbl.class))).thenAnswer(invocation -> {
            LogicalBuTbl inserted = invocation.getArgument(0);
            inserted.setId(99L);
            return 1;
        });
        doAnswer(invocation -> {
            DalQuery<?> query = invocation.getArgument(0);
            return query.doQuery();
        }).when(queryHandler).handleInsert(any());
        LogicalBuOrgTblDao orgDao = mock(LogicalBuOrgTblDao.class);
        when(orgDao.createLocal()).thenReturn(new LogicalBuOrgTbl());
        ReflectionTestUtils.setField(logicalBuService, "dao", buDao);
        ReflectionTestUtils.setField(logicalBuService, "logicalBuOrgTblDao", orgDao);

        LogicalBuModel created = new LogicalBuModel();
        created.setId(99L);
        created.setName("TFS_UAT_1");
        created.setTfsFsId("1");
        created.setActive(true);
        doReturn(created).when(logicalBuService).findById(99L);

        LogicalBuModel model = new LogicalBuModel();
        model.setName("TFS_UAT_1");
        model.setTfsFsId("1");
        model.setActive(true);
        model.setDescription("desc");

        logicalBuService.create(model);

        ArgumentCaptor<LogicalBuTbl> captor = ArgumentCaptor.forClass(LogicalBuTbl.class);
        verify(buDao).insert(captor.capture());
        Assert.assertNotNull(captor.getValue().getDataChangeLastTime());
    }

    @Test
    public void testReplaceOrgMappingsSkipsEmptyOrgIds() throws DalException {
        LogicalBuOrgTblDao orgDao = mock(LogicalBuOrgTblDao.class);
        when(orgDao.createLocal()).thenReturn(new LogicalBuOrgTbl());
        ReflectionTestUtils.setField(logicalBuService, "logicalBuOrgTblDao", orgDao);

        ReflectionTestUtils.invokeMethod(logicalBuService, "replaceOrgMappings", 7L, Lists.newArrayList(0L, null));

        verify(queryHandler).handleDelete(any(), eq(true));
        verify(orgDao, never()).insertBatch(any(LogicalBuOrgTbl[].class));
    }
}
