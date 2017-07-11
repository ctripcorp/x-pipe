package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
public class SentinelServiceImplTest extends AbstractServiceImplTest {

    @Autowired
    private SentinelServiceImpl sentinelService;

    private List<SetinelTbl> sentinels = new LinkedList<>();

    @Before
    public void beforeSentinelServiceImplTest() {

        int size = randomInt(0, 100);
        for (int i = 0; i < size; i++) {
            sentinels.add(new SetinelTbl().setSetinelId(i));
        }
    }

    @Test
    public void testAllSentinelsByDc() {

        int dcCount = 5;
        int sentinelsEachDc = 5;

        for (int i = 0; i < dcCount; i++) {
            for (int j = 0; j < sentinelsEachDc; j++) {
                sentinelService.insert(new SetinelTbl().setDcId(i).setSetinelAddress("desc").setSetinelDescription(getTestName()));
            }
        }


        Map<Long, List<SetinelTbl>> allSentinelsByDc = sentinelService.allSentinelsByDc();

        Assert.assertEquals(dcCount, allSentinelsByDc.size());

        allSentinelsByDc.forEach((dcId, sentinels) -> {
            Assert.assertTrue(sentinels.size() >= sentinelsEachDc);
        });

    }

    @Test
    public void testRandom() {

        int testCount = 1 << 20;


        Set<Integer> all = new HashSet<>();

        for (int i = 0; i < testCount; i++) {

            SetinelTbl random = sentinelService.random(sentinels);
            all.add((int) random.getSetinelId());
        }

        Assert.assertEquals(sentinels.size(), all.size());

    }
}
