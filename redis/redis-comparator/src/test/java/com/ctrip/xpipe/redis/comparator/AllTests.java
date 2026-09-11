package com.ctrip.xpipe.redis.comparator;

import com.ctrip.xpipe.redis.comparator.compare.ShardComparatorTest;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfigTest;
import com.ctrip.xpipe.redis.comparator.spring.ComparatorContextConfigTest;
import com.ctrip.xpipe.redis.comparator.stream.KeeperReplStreamTest;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBufferTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ComparatorConfigTest.class,
        ComparatorContextConfigTest.class,
        StreamRingBufferTest.class,
        ShardComparatorTest.class,
        KeeperReplStreamTest.class
})
public class AllTests {
}
