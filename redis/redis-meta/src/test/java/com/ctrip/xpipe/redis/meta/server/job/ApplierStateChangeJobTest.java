package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;

/**
 * @author ayq
 * <p>
 * 2022/4/17 17:32
 */
@RunWith(MockitoJUnitRunner.class)
public class ApplierStateChangeJobTest extends AbstractMetaServerTest {

    private ApplierStateChangeJob job;
    private List<ApplierMeta> appliers;
    private int delayBaseMilli = 200;
    private int retryTimes = 1;
    private String sid = "a1";
    private GtidSet gtidSet;

    @Mock
    private Command<?> activeSuccessCommand;

    @Before
    public void beforeApplierStateChangeJobTest() throws Exception {

        appliers = new LinkedList<>();

        appliers = createRandomAppliers(2);

        gtidSet = new GtidSet("a1:1-10:15-20");

        job = new ApplierStateChangeJob(appliers, new Pair<>("localhost", randomPort()),
                sid, gtidSet, null, getXpipeNettyClientKeyedObjectPool(), delayBaseMilli, retryTimes,
                scheduled, executors);
    }

    @Test
    public void testHookSuccess() throws Exception {

        startServer(appliers.get(0).getPort(), "+OK\r\n");
        startServer(appliers.get(1).getPort(), "+OK\r\n");

        job.setActiveSuccessCommand(activeSuccessCommand);

        job.execute().get(2000, TimeUnit.MILLISECONDS);

        verify(activeSuccessCommand).execute();
    }

    @Test
    public void testHookFail() throws InterruptedException, ExecutionException, TimeoutException {

        job.setActiveSuccessCommand(activeSuccessCommand);

        try {
            job.execute().get(delayBaseMilli, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (TimeoutException e) {
        }

        verifyNoMoreInteractions(activeSuccessCommand);

    }
}