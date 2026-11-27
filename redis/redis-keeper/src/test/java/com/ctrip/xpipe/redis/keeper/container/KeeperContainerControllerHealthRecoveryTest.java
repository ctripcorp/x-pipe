package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KeeperContainerControllerHealthRecoveryTest {

    @Mock
    private KeeperContainerService keeperContainerService;

    private KeeperContainerController controller;

    @Before
    public void setUp() {
        controller = new KeeperContainerController();
        ReflectionTestUtils.setField(controller, "keeperContainerService", keeperContainerService);
    }

    @Test
    public void testRecoverHealthStateReturnsOk() {
        KeeperTransMeta request = new KeeperTransMeta();
        request.setReplId(1L);

        Assert.assertEquals("OK", controller.recoverHealthState(request));

        verify(keeperContainerService).recoverHealthState(ReplId.from(1L));
    }

    @Test
    public void testRecoverHealthStateReturnsSpecificFailureReason() {
        KeeperTransMeta request = new KeeperTransMeta();
        request.setReplId(1L);
        String reason = "Recover health for repl_1 failed to publish election data";
        doThrow(new RedisKeeperRuntimeException(
                new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION, reason), null))
                .when(keeperContainerService).recoverHealthState(ReplId.from(1L));

        Assert.assertEquals(reason, controller.recoverHealthState(request));
    }

    @Test
    public void testRecoverHealthStateRejectsMissingReplId() {
        Assert.assertEquals("replId is required", controller.recoverHealthState(new KeeperTransMeta()));
        Assert.assertEquals("replId is required", controller.recoverHealthState(null));
    }
}
