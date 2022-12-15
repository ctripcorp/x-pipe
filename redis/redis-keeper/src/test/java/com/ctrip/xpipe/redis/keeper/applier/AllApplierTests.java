package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.redis.keeper.applier.command.DefaultApplierCommandTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceControllerTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.SequenceCommandTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.StubbornCommandTest;
import com.ctrip.xpipe.redis.keeper.applier.threshold.AbstractThresholdTest;
import com.ctrip.xpipe.redis.keeper.applier.threshold.QPSThresholdTest;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultCommandDispatcherTest;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultXsyncReplicationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({

        DefaultApplierCommandTest.class,

        AbstractThresholdTest.class,
        QPSThresholdTest.class,

        DefaultSequenceControllerTest.class,
        SequenceCommandTest.class,
        StubbornCommandTest.class,

        DefaultCommandDispatcherTest.class,
        DefaultXsyncReplicationTest.class,

        AbstractInstanceNodeTest.class,
        DefaultApplierServerTest.class,
})
public class AllApplierTests {

}
