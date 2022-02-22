package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.redis.keeper.applier.client.DoNothingRedisClientTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceControllerTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.SequenceCommandTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.StubbornCommandTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        DoNothingRedisClientTest.class,

        DefaultSequenceControllerTest.class,
        SequenceCommandTest.class,
        StubbornCommandTest.class,
})
public class AllApplierTests {

}
