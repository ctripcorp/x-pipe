package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.TestCommand;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerService;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * @author ayq
 * <p>
 * 2022/4/7 20:20
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultApplierStateControllerTest extends AbstractMetaServerTest {

    private DefaultApplierStateController defaultApplierStateController;

    private int sllepInterval = 0;

    @Mock
    private ApplierContainerService appliercontainerService;

    private Supplier<Command<?>> addCommandSupplier;

    private TestCommand addCommand = new TestCommand("success", sllepInterval);

    private TestCommand deleteCommand = new TestCommand("success", sllepInterval);

    @Before
    public void beforeDefaultApplierStateControllerTest() throws Exception {
        defaultApplierStateController = new DefaultApplierStateController() {
            @Override
            protected Command<?> createAddApplierCommand(ApplierContainerService applierContainerService, ApplierTransMeta applierTransMeta, ScheduledExecutorService scheduled, int addApplierSuccessTimeoutMilli) {
                return addCommand;
            }

            @Override
            protected Command<?> createDeleteApplierCommand(ApplierContainerService applierContainerService, ApplierTransMeta applierTransMeta, ScheduledExecutorService scheduled, int removeApplierSuccessTimeoutMilli) {
                return deleteCommand;
            }

            @Override
            protected ApplierContainerService getApplierContainerService(ApplierTransMeta applierTransMeta) {
                return appliercontainerService;
            }
        };

        defaultApplierStateController.setExecutors(executors);
        LifecycleHelper.initializeIfPossible(defaultApplierStateController);
        LifecycleHelper.startIfPossible(defaultApplierStateController);
    }

    @Test
    public void testDelete() throws TimeoutException {
        Assert.assertFalse(deleteCommand.isBeginExecute());
        defaultApplierStateController.removeApplier(new ApplierTransMeta(getClusterDbId(), getShardDbId(), new ApplierMeta()));
        waitConditionUntilTimeOut(() -> deleteCommand.isBeginExecute(), 1000);
    }

    @Test
    public void testAdd() {
        addCommandSupplier = new Supplier<Command<?>>() {
            @Override
            public Command<?> get() {
                return addCommand;
            }
        };
        Assert.assertFalse(addCommand.isBeginExecute());

        defaultApplierStateController.addApplier(new ApplierTransMeta(getClusterDbId(), getShardDbId(), new ApplierMeta()));
        sleep(50);
        Assert.assertTrue(addCommand.isBeginExecute());
    }
}