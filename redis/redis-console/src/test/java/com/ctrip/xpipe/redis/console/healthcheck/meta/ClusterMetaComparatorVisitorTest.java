package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class ClusterMetaComparatorVisitorTest extends AbstractRedisTest {

    @Test
    public void testVisitModified() {
        boolean trueVal = true, falseVal = false;
        System.out.println(true ^ false);
        System.out.println(false ^ false);
        System.out.println(true ^ true);
        System.out.println(false ^ true);
    }
}