package com.ctrip.xpipe.redis.comparator;

import com.ctrip.xpipe.redis.comparator.config.ComparatorConfigTest;
import com.ctrip.xpipe.redis.comparator.spring.ComparatorContextConfigTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ComparatorConfigTest.class,
        ComparatorContextConfigTest.class
})
public class AllTests {
}
