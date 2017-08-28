package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 06, 2017
 */
public class KeepercontainerServiceImplTest extends AbstractServiceImplTest{


    @Autowired
    private KeepercontainerServiceImpl keepercontainerService;


    @Before
    public void beforeKeepercontainerServiceImplTest(){

    }

    @Test
    public void testKeeperCount(){

        List<KeepercontainerTbl> keeperCount = keepercontainerService.findKeeperCount(dcNames[0]);
        keeperCount.forEach((keepercontainerTbl) -> {
            logger.info("{}", keepercontainerTbl);
        });

    }

    @Test
    public void testFindKeeperCountByCluster() {
        long orgId = 2L;
        List<KeepercontainerTbl> keeperCount = keepercontainerService.findKeeperCountByClusterOrg(dcNames[0], orgId);
        keeperCount.forEach(kc -> logger.info("{}", kc));
        Assert.assertTrue(keeperCount.stream().allMatch(kc->kc.getKeepercontainerOrgId() == orgId));
        logger.info("------------------------------------------------------------------");
        long noneOrgId = 0L;
        keeperCount = keepercontainerService.findKeeperCountByClusterOrg(dcNames[0], noneOrgId);
        keeperCount.forEach(kc -> logger.info("{}", kc));
        Assert.assertTrue(keeperCount.stream().allMatch(kc->kc.getKeepercontainerOrgId() == noneOrgId));
    }

    @Test
    public  void testFindAllActiveByDcName(){

        String dcName = dcNames[0];
        List<KeepercontainerTbl> allByDcName = keepercontainerService.findAllByDcName(dcName);

        int size = allByDcName.size();
        Assert.assertTrue(size > 0);
        for(KeepercontainerTbl keepercontainerTbl : allByDcName){
            Assert.assertTrue(keepercontainerTbl.isKeepercontainerActive());

            keepercontainerTbl.setKeepercontainerActive(false);
            keepercontainerService.update(keepercontainerTbl);
        }

        List<KeepercontainerTbl> allActiveByDcName = keepercontainerService.findAllActiveByDcName(dcName);
        Assert.assertEquals(0, allActiveByDcName.size());

        allByDcName = keepercontainerService.findAllByDcName(dcName);
        Assert.assertEquals(size, allByDcName.size());

    }
}
