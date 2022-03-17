package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.SentinelTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 * <p>
 * Jul 11, 2017
 */
public class SentinelServiceImplTest extends AbstractServiceImplTest {

    @Autowired
    private SentinelServiceImpl sentinelService;

    private List<SetinelTbl> sentinels = new LinkedList<>();


    @Before
    public void beforeSentinelServiceImplTest() {
        MockitoAnnotations.initMocks(this);
        int size = randomInt(0, 100);
        for (int i = 0; i < size; i++) {
            sentinels.add(new SetinelTbl().setSetinelId(i));
        }
    }

    @Test
    public void deleteAndQuery() {
        List<SentinelTbl> sentinelTbls = sentinelService.findAll();
        Assert.assertEquals(8, sentinelTbls.size());

        SentinelTbl sentinelTbl = sentinelTbls.get(0);
        List<SentinelTbl> groupSentinelsBefore = sentinelService.findBySentinelGroupId(sentinelTbl.getSentinelGroupId());
        sentinelService.delete(sentinelTbl.getSentinelId());
        List<SentinelTbl> groupSentinelsAfter = sentinelService.findBySentinelGroupId(sentinelTbl.getSentinelGroupId());
        Assert.assertEquals(groupSentinelsBefore.size(), groupSentinelsAfter.size() + 1);

        List<SentinelTbl> deleted = sentinelService.findBySentinelGroupIdDeleted(sentinelTbl.getSentinelGroupId());
        Assert.assertEquals(1, deleted.size());
        Assert.assertEquals(sentinelTbl.getSentinelPort(), deleted.get(0).getSentinelPort());

        sentinelService.reheal(sentinelTbl.getSentinelId());
        List<SentinelTbl> rehealed = sentinelService.findBySentinelGroupId(sentinelTbl.getSentinelGroupId());
        Assert.assertEquals(groupSentinelsBefore.size(), rehealed.size());
    }

    @Test
    public void updateAndQuery() {
        SentinelTbl query = sentinelService.findByIpPort("127.0.0.1", 5000);
        Assert.assertEquals(1, query.getSentinelGroupId());

        query.setSentinelPort(5001).setSentinelIp("127.0.0.2");
        sentinelService.updateByPk(query);

        Assert.assertNull(sentinelService.findByIpPort("127.0.0.1", 5000));
        Assert.assertNotNull(sentinelService.findByIpPort("127.0.0.2", 5001));
    }

    @Test
    public void findByDc(){
       List<SentinelTbl> result=sentinelService.findAllByDcName("oy");
       Assert.assertEquals(3,result.size());

        result=sentinelService.findAllByDcName("oy2");
        Assert.assertEquals(0,result.size());

        List<SentinelTbl> resultWithDcName= sentinelService.findAllWithDcName();
        Assert.assertEquals(8,resultWithDcName.size());

        for(SentinelTbl sentinelTbl:resultWithDcName){
            if(sentinelTbl.getDcId()==1){
                Assert.assertEquals("jq",sentinelTbl.getDcInfo().getDcName());
            }else if(sentinelTbl.getDcId()==2){
                Assert.assertEquals("oy",sentinelTbl.getDcInfo().getDcName());
            }else{
                Assert.assertEquals("fra",sentinelTbl.getDcInfo().getDcName());
            }
        }
    }

    @Test
    public void insertAndQuery(){

        List<SentinelTbl> groupSentinelsBefore = sentinelService.findBySentinelGroupId(1);

        SentinelTbl newSentinel=new SentinelTbl().setSentinelIp("127.0.0.1").setSentinelPort(5003).setSentinelGroupId(1).setDcId(1);
        newSentinel= sentinelService.insert(newSentinel);
        Assert.assertTrue(newSentinel.getSentinelId()>0);

        List<SentinelTbl> groupSentinelsAfter = sentinelService.findBySentinelGroupId(1);

        Assert.assertEquals(groupSentinelsBefore.size()+1,groupSentinelsAfter.size());
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

}
