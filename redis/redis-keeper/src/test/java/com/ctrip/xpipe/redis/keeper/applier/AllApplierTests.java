package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceControllerTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.SequenceCommandTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.StubbornCommandTest;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultCommandDispatcherTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({

        //DefaultApplierCommandTest.class,

        DefaultSequenceControllerTest.class,
        SequenceCommandTest.class,
        StubbornCommandTest.class,

        DefaultCommandDispatcherTest.class,

        AbstractInstanceNodeTest.class,
        //DefaultApplierServerTest.class,
})
public class AllApplierTests {

}
