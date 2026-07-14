package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblEntity;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DcClusterShardServiceImplBatchUpdateOperatingUntilTest {

    @Mock
    private DcClusterShardTblDao dao;

    private DcClusterShardServiceImpl service;

    @Before
    public void setUp() {
        service = new DcClusterShardServiceImpl();
        ReflectionTestUtils.setField(service, "dao", dao);
    }

    @Test
    public void testFindDcClusterShardsByNames() throws DalException {
        DcClusterShardTbl shard1 = new DcClusterShardTbl().setDcClusterShardId(11L).setShardName("shard1");
        DcClusterShardTbl shard2 = new DcClusterShardTbl().setDcClusterShardId(22L).setShardName("shard2");
        when(dao.findDcClusterShardsByNames(eq("jq"), eq("cluster1"), eq(Arrays.asList("shard1", "shard2")),
                eq(DcClusterShardTblEntity.READSET_FULL))).thenReturn(Arrays.asList(shard1, shard2));

        List<DcClusterShardTbl> matched = service.findDcClusterShardsByNames("jq", "cluster1",
                Arrays.asList("shard1", "shard2"));

        Assert.assertEquals(2, matched.size());
        Assert.assertEquals(11L, matched.get(0).getDcClusterShardId());
    }

    @Test
    public void testFindDcClusterShardsByNamesReturnsEmptyWhenShardNamesEmpty() {
        Assert.assertTrue(service.findDcClusterShardsByNames("jq", "cluster1", Collections.emptyList()).isEmpty());
        verifyNoInteractions(dao);
    }

    @Test
    public void testUpdateOperatingUntilByIds() throws DalException {
        when(dao.updateOperatingUntilByIds(org.mockito.ArgumentMatchers.any(DcClusterShardTbl.class),
                eq(DcClusterShardTblEntity.UPDATESET_OPERATING_UNTIL))).thenReturn(2);

        Date until = new Date(System.currentTimeMillis() + 3600_000L);
        int affected = service.updateOperatingUntilByIds(Arrays.asList(11L, 22L), until);

        Assert.assertEquals(2, affected);
        ArgumentCaptor<DcClusterShardTbl> captor = ArgumentCaptor.forClass(DcClusterShardTbl.class);
        verify(dao).updateOperatingUntilByIds(captor.capture(), eq(DcClusterShardTblEntity.UPDATESET_OPERATING_UNTIL));
        Assert.assertEquals(until, captor.getValue().getOperatingUntil());
        Assert.assertEquals(Arrays.asList(11L, 22L), captor.getValue().getDcClusterShardIds());
    }

    @Test
    public void testUpdateOperatingUntilByIdsReturnsZeroWhenIdsEmpty() throws DalException {
        Assert.assertEquals(0, service.updateOperatingUntilByIds(Collections.emptyList(),
                DateTimeUtils.DEFAULT_OPERATING_UNTIL));
        verifyNoInteractions(dao);
    }
}
