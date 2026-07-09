package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.unidal.dal.jdbc.DalException;

import java.util.Collections;
import java.util.Date;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DcClusterShardServiceImplInsertBatchTest {

    @Mock
    private DcClusterShardTblDao dao;

    private DcClusterShardServiceImpl service;

    @Before
    public void setUp() throws DalException {
        service = new DcClusterShardServiceImpl();
        ReflectionTestUtils.setField(service, "dao", dao);
        when(dao.insertBatch(any(DcClusterShardTbl[].class))).thenReturn(new int[] {1});
    }

    @Test
    public void testInsertBatchFillsNullOperatingUntil() throws DalException {
        DcClusterShardTbl proto = new DcClusterShardTbl().setDcClusterId(1L).setShardId(2L);
        service.insertBatch(Collections.singletonList(proto));

        Assert.assertEquals(DateTimeUtils.DEFAULT_OPERATING_UNTIL, proto.getOperatingUntil());

        ArgumentCaptor<DcClusterShardTbl[]> captor = ArgumentCaptor.forClass(DcClusterShardTbl[].class);
        verify(dao).insertBatch(captor.capture());
        Assert.assertEquals(DateTimeUtils.DEFAULT_OPERATING_UNTIL, captor.getValue()[0].getOperatingUntil());
    }

    @Test
    public void testInsertBatchKeepsNonNullOperatingUntil() throws DalException {
        Date customUntil = new Date(System.currentTimeMillis() + 3600_000L);
        DcClusterShardTbl proto = new DcClusterShardTbl()
                .setDcClusterId(1L)
                .setShardId(2L)
                .setOperatingUntil(customUntil);
        service.insertBatch(Collections.singletonList(proto));

        Assert.assertEquals(customUntil, proto.getOperatingUntil());

        ArgumentCaptor<DcClusterShardTbl[]> captor = ArgumentCaptor.forClass(DcClusterShardTbl[].class);
        verify(dao).insertBatch(captor.capture());
        Assert.assertEquals(customUntil, captor.getValue()[0].getOperatingUntil());
    }
}
