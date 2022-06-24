package com.ctrip.xpipe.redis.core.redis.rdb;

import com.ctrip.xpipe.redis.core.redis.rdb.encoding.IntsetTest;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.StreamListpackTest;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParserTest;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbAuxParserTest;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbStringParserTest;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.ZiplistTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author lishanglin
 * date 2022/6/5
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        DefaultRdbParserTest.class,
        RdbAuxParserTest.class,
        RdbStringParserTest.class,

        ZiplistTest.class,
        IntsetTest.class,
        StreamListpackTest.class
})
public class AllRdbTests {
}
