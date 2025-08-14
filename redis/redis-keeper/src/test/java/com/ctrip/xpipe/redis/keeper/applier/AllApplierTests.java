package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.redis.keeper.applier.command.TransactionCommandTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceControllerTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.SequenceCommandTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.StubbornCommandTest;
import com.ctrip.xpipe.redis.keeper.applier.threshold.AbstractThresholdTest;
import com.ctrip.xpipe.redis.keeper.applier.threshold.BytesPerSecondThresholdTest;
import com.ctrip.xpipe.redis.keeper.applier.threshold.GTIDDistanceThresholdTest;
import com.ctrip.xpipe.redis.keeper.applier.threshold.QPSThresholdTest;
import com.ctrip.xpipe.redis.keeper.applier.sync.DefaultCommandDispatcherTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({

        AbstractThresholdTest.class,
        GTIDDistanceThresholdTest.class,
        QPSThresholdTest.class,
        BytesPerSecondThresholdTest.class,

        DefaultSequenceControllerTest.class,
        SequenceCommandTest.class,
        StubbornCommandTest.class,

        DefaultCommandDispatcherTest.class,
        AbstractInstanceNodeTest.class,
        DefaultApplierServerTest.class,

        TransactionCommandTest.class,
})
public class AllApplierTests {

}
