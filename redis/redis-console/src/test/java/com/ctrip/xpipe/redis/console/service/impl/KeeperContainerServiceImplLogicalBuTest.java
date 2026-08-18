package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KeeperContainerServiceImplLogicalBuTest {

    @InjectMocks
    private KeeperContainerServiceImpl keeperContainerService;

    @Mock
    private LogicalBuService logicalBuService;

    @Test
    public void testValidateAndSetLogicalBuTfsUnbindSetsZero() {
        KeepercontainerTbl proto = new KeepercontainerTbl();
        ReflectionTestUtils.invokeMethod(keeperContainerService, "validateAndSetLogicalBu",
                proto, "tfs", null);
        Assert.assertEquals(0L, proto.getLogicalBuId());
        verifyNoInteractions(logicalBuService);
    }

    @Test
    public void testValidateAndSetLogicalBuTfsWithBinding() {
        when(logicalBuService.findById(42L)).thenReturn(new LogicalBuModel().setId(42L));
        KeepercontainerTbl proto = new KeepercontainerTbl();
        ReflectionTestUtils.invokeMethod(keeperContainerService, "validateAndSetLogicalBu",
                proto, "TFS-xxx", 42L);
        Assert.assertEquals(42L, proto.getLogicalBuId());
        verify(logicalBuService).findById(42L);
    }

    @Test(expected = BadRequestException.class)
    public void testValidateAndSetLogicalBuRejectsMissingBu() {
        when(logicalBuService.findById(99L)).thenThrow(new BadRequestException("Logical BU not found: 99"));
        KeepercontainerTbl proto = new KeepercontainerTbl();
        ReflectionTestUtils.invokeMethod(keeperContainerService, "validateAndSetLogicalBu",
                proto, "tfs", 99L);
    }

    @Test
    public void testValidateAndSetLogicalBuBmUnbindSetsZero() {
        KeepercontainerTbl proto = new KeepercontainerTbl();
        ReflectionTestUtils.invokeMethod(keeperContainerService, "validateAndSetLogicalBu",
                proto, "DEFAULT", null);
        Assert.assertEquals(0L, proto.getLogicalBuId());
        verifyNoInteractions(logicalBuService);
    }

    @Test
    public void testValidateAndSetLogicalBuNonPositiveSetsZero() {
        KeepercontainerTbl proto = new KeepercontainerTbl();
        ReflectionTestUtils.invokeMethod(keeperContainerService, "validateAndSetLogicalBu",
                proto, "DEFAULT", -1L);
        Assert.assertEquals(0L, proto.getLogicalBuId());
        verifyNoInteractions(logicalBuService);
    }
}
