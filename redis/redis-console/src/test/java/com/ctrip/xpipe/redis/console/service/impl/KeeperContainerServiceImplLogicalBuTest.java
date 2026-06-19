package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(MockitoJUnitRunner.class)
public class KeeperContainerServiceImplLogicalBuTest {

    @InjectMocks
    private KeeperContainerServiceImpl keeperContainerService;

    @Test(expected = BadRequestException.class)
    public void testValidateAndSetLogicalBuTfsRequiresBinding() {
        KeepercontainerTbl proto = new KeepercontainerTbl();
        ReflectionTestUtils.invokeMethod(keeperContainerService, "validateAndSetLogicalBu",
                proto, "tfs", null);
    }

    @Test
    public void testValidateAndSetLogicalBuTfsWithBinding() {
        KeepercontainerTbl proto = new KeepercontainerTbl();
        ReflectionTestUtils.invokeMethod(keeperContainerService, "validateAndSetLogicalBu",
                proto, "TFS-xxx", 42L);
        Assert.assertEquals(42L, proto.getLogicalBuId());
    }

    @Test
    public void testValidateAndSetLogicalBuBmUnbindSetsZero() {
        KeepercontainerTbl proto = new KeepercontainerTbl();
        ReflectionTestUtils.invokeMethod(keeperContainerService, "validateAndSetLogicalBu",
                proto, "DEFAULT", null);
        Assert.assertEquals(0L, proto.getLogicalBuId());
    }
}
