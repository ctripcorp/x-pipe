package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.model.SentinelModel;
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

        int testCount = 1 << 10;


        Set<Long> all = new HashSet<>();

        for (int i = 0; i < testCount; i++) {

            SetinelTbl random = sentinelService.random(sentinels);
            all.add(random.getSetinelId());
        }

        Assert.assertEquals(sentinels.size(), all.size());

    }

    @Test
    public void testGetSentinelUsage() {
        logger.info("{}", sentinelService.allSentinelsByDc());
        logger.info("{}", sentinelService.getAllSentinelsUsage());
    }

    @Test
    public void testUpdateSentinel() {
        List<SetinelTbl> sentinels = sentinelService.findAllByDcName(dcNames[0]);
        Assert.assertFalse(sentinels.isEmpty());
        SetinelTbl target = sentinels.get(0);
        Assert.assertNotNull(target);

        String prevAddr = target.getSetinelAddress();

        SentinelModel model = new SentinelModel(target);
        model.getSentinels().remove(model.getSentinels().size() - 1);
        model.getSentinels().add(HostPort.fromString(String.join(":", "192.168.0.1", "" + randomPort())));

        SentinelModel updatedModel = sentinelService.updateSentinelTblAddr(model);

        SetinelTbl updated = sentinelService.find(target.getSetinelId());

        Assert.assertNotEquals(prevAddr, updated.getSetinelAddress());

        Assert.assertEquals(updatedModel, new SentinelModel(updated));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSentinelNotExist() {
        List<SetinelTbl> sentinels = sentinelService.findAllByDcName(dcNames[0]);
        Assert.assertFalse(sentinels.isEmpty());
        SetinelTbl target = sentinels.get(0);
        Assert.assertNotNull(target);

        SentinelModel model = new SentinelModel(target);
        model.getSentinels().remove(model.getSentinels().size() - 1);
        model.getSentinels().add(HostPort.fromString(String.join(":", "192.168.0.1", "" + randomPort())));

        long unSelectedBitSentinelId = 0;
        for(SetinelTbl setinelTbl : sentinels) {
            unSelectedBitSentinelId |= setinelTbl.getSetinelId();
        }

        long targetId = Math.abs(Long.MAX_VALUE ^ unSelectedBitSentinelId);
        model.setId(targetId);

        try {
            sentinelService.updateSentinelTblAddr(model);
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        }
    }
}
