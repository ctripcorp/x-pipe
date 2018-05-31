package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 28, 2018
 */
public class TimeTest {
    @Test
    public void test() {
        long time = 1527560514L;
        System.out.println(DateTimeUtils.timeAsString(time));
    }
}
