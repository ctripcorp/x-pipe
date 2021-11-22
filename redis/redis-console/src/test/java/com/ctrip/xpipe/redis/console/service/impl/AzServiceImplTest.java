package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.AzCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.AzTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.io.IOException;
import java.util.List;

/**
 * @author: Song_Yu
 * @date: 2021/11/8
 */
public class AzServiceImplTest extends AbstractServiceImplTest{

    @Autowired
    private AzServiceImpl azService;

    @Autowired
    private DcServiceImpl dcService;

    @Autowired
    private KeeperContainerServiceImpl keeperContainerService;

    @Autowired
    private KeeperServiceImpl keeperService;

    @Autowired
    private RedisServiceImpl redisService;

    @Test(expected = IllegalArgumentException.class)
    public void testAddAzFailByWrongDc() {
        String dcName = "sha";
        try {
            addAvailableZone(dcName, true, "G", "zone for G");
        } catch (Exception e) {
            Assert.assertEquals("DC name " + dcName + " does not exist", e.getMessage());
            throw e;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddAzFailByAlreadyExist() {
        String azName = "JQ-G";
        addAvailableZone(dcNames[0], true, azName, "Zone for G");

        try {
            addAvailableZone(dcNames[0], true, azName, "Zone for G");
        } catch (Exception e) {
            Assert.assertEquals("available zone : " + azName + " already exists", e.getMessage());
            throw e;
        }
    }

    @Test
    public void testUpdateAzSuccess() throws DalException {
        String azName = "JQ-G";
        long dc_id = dcService.findByDcName(dcNames[0]).getId();
        addAvailableZone(dcNames[0], true, azName, "zone for G");

        AzTbl at = azService.getAvailableZoneByAzName(azName);
        Assert.assertEquals("zone for G", at.getDescription());

        AzCreateInfo createInfo = new AzCreateInfo()
                .setDcName(dcNames[0])
                .setActive(false)
                .setAzName(azName)
                .setDescription("zone for F");

        azService.updateAvailableZone(createInfo);

        at = azService.getAvailableZoneByAzName(azName);
        Assert.assertEquals("zone for F", at.getDescription());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAzFailByWrongDc(){
        String azName = "JQ-G";
        String wrongDcName = "XY";
        addAvailableZone(dcNames[0], true, azName, "zone for G");

        AzTbl at = azService.getAvailableZoneByAzName(azName);
        Assert.assertEquals("zone for G", at.getDescription());

        AzCreateInfo createInfo = new AzCreateInfo()
                .setDcName(wrongDcName)
                .setActive(false)
                .setAzName(azName)
                .setDescription("zone for F");

        try {
            azService.updateAvailableZone(createInfo);
        } catch (Exception e) {
            Assert.assertEquals("DC name " + wrongDcName +  " does not exist" , e.getMessage());
            throw e;
        }
    }


    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAzFailByNonExistZone(){
        String azName = "G";
        AzCreateInfo createInfo = new AzCreateInfo()
                .setDcName(dcNames[0])
                .setActive(true)
                .setAzName(azName)
                .setDescription("zone for F");

        try {
            azService.updateAvailableZone(createInfo);
        } catch (Exception e) {
            Assert.assertEquals("availablezone " + azName +  " not found" , e.getMessage());
            throw e;
        }
    }

    @Test
    public void testDeleteAzSuccess(){
        String azName = "JQ-G";
        addAvailableZone(dcNames[0], true, azName, "zone for G");

        AzTbl at = azService.getAvailableZoneByAzName(azName);
        Assert.assertNotNull(at);

        azService.deleteAvailableZoneByName(azName);

        at = azService.getAvailableZoneByAzName(azName);
        Assert.assertNull(at);
    }

    @Test(expected = BadRequestException.class)
    public void testDeleteAzFailByNonExistAz(){
        String azName = "G";
        try {
            azService.deleteAvailableZoneByName(azName);
        } catch (Exception e) {
            Assert.assertEquals("availablezone " + azName +  " not found" , e.getMessage());
            throw e;
        }
    }

    @Test(expected = BadRequestException.class)
    public void testDeleteAzFailByStillHasKeeperContainer(){
        String azName = "A";
        AzTbl at = azService.getAvailableZoneByAzName(azName);
        Assert.assertNotNull(at);

        try {
            azService.deleteAvailableZoneByName(azName);
        } catch (BadRequestException e) {
            Assert.assertEquals("This keepercontainer has keepers", e.getMessage());
            at = azService.getAvailableZoneByAzName(azName);
            Assert.assertNotNull(at);
            throw  e;
        }

    }

    @Test
    public void TestGetAzByDc(){
        addAvailableZone(dcNames[0], true, "JQ-A", "Zone for A");
        addAvailableZone(dcNames[0], true, "JQ-B", "Zone for C");
        addAvailableZone(dcNames[0], true, "JQ-C", "Zone for C");
        addAvailableZone(dcNames[0], false, "JQ-D", "Zone for D");

        addAvailableZone(dcNames[1], true, "OY-A", "Zone for A");

        List<AzCreateInfo> createInfoList = azService.getDcAvailableZones(dcNames[0]);
        Assert.assertEquals(4, createInfoList.size());

        createInfoList = azService.getDcAvailableZones(dcNames[1]);
        Assert.assertEquals(1, createInfoList.size());

    }

    @Test
    public void TestGetAllAzs(){
        List<AzCreateInfo> createInfoList = azService.getAllAvailableZones();
        Assert.assertEquals(3, createInfoList.size());
    }


    private void addAvailableZone(String dcName, boolean isActive, String azName, String desc){
        AzCreateInfo createInfo = new AzCreateInfo()
                .setDcName(dcName)
                .setActive(isActive)
                .setAzName(azName)
                .setDescription(desc);

        azService.addAvailableZone(createInfo);

        Assert.assertEquals(true, azService.availableZoneIsExist(createInfo));
    }

    @Override
    protected String prepareDatas() throws IOException {
        return  prepareDatasFromFile("src/test/resources/available-zone-service-impl-test.sql");
    }
}
