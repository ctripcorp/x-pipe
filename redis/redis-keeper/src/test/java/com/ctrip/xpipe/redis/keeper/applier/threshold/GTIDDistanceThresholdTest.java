package com.ctrip.xpipe.redis.keeper.applier.threshold;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestSleepCommand;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Slight
 * <p>
 * Jan 17, 2023 19:43
 */
public class GTIDDistanceThresholdTest extends AbstractTest {

    @Test
    public void test() throws InterruptedException {
        GTIDDistanceThreshold threshold = new GTIDDistanceThreshold(2000);
        int fail = -1;
        for (int i = 1; i < 2030; i++) {
            if (!threshold.tryPass(i, 100)) {
                fail = i;
                break;
            }
        }
        assertEquals(2001, fail);

        threshold.submit(2000);

        for (int i = 1; i < 4030; i++) {
            if (!threshold.tryPass(i, 100)) {
                fail = i;
                break;
            }
        }
        assertEquals(4001, fail);
    }
}