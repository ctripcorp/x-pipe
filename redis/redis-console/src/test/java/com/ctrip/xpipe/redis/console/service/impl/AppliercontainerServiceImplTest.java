package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.AppliercontainerCreateInfo;
import com.ctrip.xpipe.redis.console.model.AppliercontainerInfoModel;
import com.ctrip.xpipe.redis.console.model.AppliercontainerTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

public class AppliercontainerServiceImplTest extends AbstractServiceImplTest {

    @Autowired
    AppliercontainerServiceImpl appliercontainerService;

    @Autowired
    OrganizationServiceImpl organizationService;

    @Test
    public void testFind() {
        for (int i = 1; i < 10; i++) {
            logger.info(appliercontainerService.findAppliercontainerTblById(i).toString());
        }
        AppliercontainerTbl appliercontainerTbl = appliercontainerService.findAppliercontainerTblById(5);
        Assert.assertNotNull(appliercontainerTbl);
        Assert.assertEquals("127.0.0.5", appliercontainerTbl.getAppliercontainerIp());
    }

    @Test
    public void testFindByDcName() {
        List<AppliercontainerTbl> appliercontainers = appliercontainerService.findAllAppliercontainerTblsByDc("oy");
        Assert.assertEquals(3, appliercontainers.size());

        List<AppliercontainerCreateInfo> appliercontainerCreateInfos = appliercontainerService.findAllAppliercontainerCreateInfosByDc("oy");
        Assert.assertEquals(3, appliercontainerCreateInfos.size());

        appliercontainers = appliercontainerService.findAllActiveAppliercontainersByDc("oy");
        Assert.assertEquals(2, appliercontainers.size());
    }

    @Test
    public void testFindByAzId() {
        List<AppliercontainerTbl> appliercontainers = appliercontainerService.findAllAppliercontainersByAz(1L);
        Assert.assertEquals(4, appliercontainers.size());
    }

    @Test
    public void testAddAppliercontainerByCreateInfo() {
        AppliercontainerCreateInfo createInfo = new AppliercontainerCreateInfo();
        createInfo.setAppliercontainerIp("1.1.1.1").setAppliercontainerPort(8080)
                .setAppliercontainerOrgId(0L).setDcName("jq").setActive(true);

        Assert.assertNull(appliercontainerService.findByIpPort("1.1.1.1", 8080));
        appliercontainerService.addAppliercontainerByCreateInfo(createInfo);

        Assert.assertNotNull(appliercontainerService.findByIpPort("1.1.1.1", 8080));

        try {
            appliercontainerService.addAppliercontainerByCreateInfo(createInfo);
        } catch (Exception e) {
            Assert.assertEquals("Appliercontainer with IP: 1.1.1.1 already exists", e.getMessage());
        }

        createInfo.setAppliercontainerOrgId(100L).setAppliercontainerIp("2.2.2.2");
        try {
            appliercontainerService.addAppliercontainerByCreateInfo(createInfo);
        } catch (Exception e) {
            Assert.assertEquals("org 100 does not exist", e.getMessage());
        }

        createInfo.setAppliercontainerOrgId(0L).setAzName("AB");
        try {
            appliercontainerService.addAppliercontainerByCreateInfo(createInfo);
        } catch (Exception e) {
            Assert.assertEquals("available zone AB does not exist", e.getMessage());
        }
    }

    @Test
    public void testDeleteAppliercontainerByCreateInfo() {
        String ip = "1.1.1.1";
        int port = 8080;
        AppliercontainerCreateInfo createInfo = new AppliercontainerCreateInfo();
        createInfo.setAppliercontainerIp(ip).setAppliercontainerPort(port)
                .setAppliercontainerOrgId(0L).setDcName("jq").setActive(true);

        Assert.assertNull(appliercontainerService.findByIpPort(ip, port));
        appliercontainerService.addAppliercontainerByCreateInfo(createInfo);

        Assert.assertNotNull(appliercontainerService.findByIpPort(ip, port));

        appliercontainerService.deleteAppliercontainerByCreateInfo(ip, port);
        Assert.assertNull(appliercontainerService.findByIpPort(ip, port));

        String wrontIp = "2.2.2.2";
        try {
            appliercontainerService.deleteAppliercontainerByCreateInfo(wrontIp, port);
        } catch (Exception e) {
            Assert.assertEquals(String.format("appliercontainer %s:%d does not exist",
                    wrontIp, port), e.getMessage());
        }

        String existApplierIp = "127.0.0.1";
        try {
            appliercontainerService.deleteAppliercontainerByCreateInfo(existApplierIp, port);
        } catch (Exception e) {
            Assert.assertEquals(String.format("This appliercontainer %s:%d is not empty, unable to delete",
                    existApplierIp, port), e.getMessage());
        }
    }

    @Test
    public void testUpdateAppliercontainerByCreateInfo() {
        String ip = "1.1.1.1";
        int port = 8080;
        AppliercontainerCreateInfo createInfo = new AppliercontainerCreateInfo();
        createInfo.setAppliercontainerIp(ip).setAppliercontainerPort(port)
                .setAppliercontainerOrgId(0L).setDcName("fra").setActive(true);

        try {
            appliercontainerService.updateAppliercontainerByCreateInfo(createInfo);
        } catch (Exception e) {
            Assert.assertEquals(String.format("appliercontainer %s:%d  not found", ip, port), e.getMessage());
        }

        Assert.assertNull(appliercontainerService.findByIpPort(ip, port));
        appliercontainerService.addAppliercontainerByCreateInfo(createInfo);

        Assert.assertNotNull(appliercontainerService.findByIpPort(ip, port));

        createInfo.setAzName("A");
        appliercontainerService.updateAppliercontainerByCreateInfo(createInfo);
        Assert.assertEquals(1, appliercontainerService.findByIpPort(ip, port).getAppliercontainerAz());

        createInfo.setAppliercontainerOrgId(2L).setAzName("AB");
        try {
            appliercontainerService.updateAppliercontainerByCreateInfo(createInfo);
        } catch (Exception e) {
            Assert.assertEquals("available zone AB does not exist", e.getMessage());
        }
    }

    @Test
    public void testFindBestAppliercontainer() {
        List<AppliercontainerTbl> bestAppliercontainers =
                appliercontainerService.findBestAppliercontainersByDcCluster("fra", "hetero-cluster");
        Assert.assertEquals(2, bestAppliercontainers.size());
        Assert.assertEquals(true, bestAppliercontainers.get(0).getAppliercontainerId() < 10);
        Assert.assertEquals(true, bestAppliercontainers.get(1).getAppliercontainerId() < 10);
        Assert.assertEquals(false, bestAppliercontainers.get(0).getAppliercontainerAz() ==
                bestAppliercontainers.get(1).getAppliercontainerAz());

        bestAppliercontainers =
                appliercontainerService.findBestAppliercontainersByDcCluster("fra", "hetero-cluster2");
        Assert.assertEquals(2, bestAppliercontainers.size());
        Assert.assertEquals(true, bestAppliercontainers.get(0).getAppliercontainerId() > 10);
        Assert.assertEquals(true, bestAppliercontainers.get(1).getAppliercontainerId() > 10);
        Assert.assertEquals(false, bestAppliercontainers.get(0).getAppliercontainerAz() ==
                bestAppliercontainers.get(1).getAppliercontainerAz());

    }

    @Test
    public void testFindAllAppliercontainerInfoModels() {
        List<AppliercontainerInfoModel> appliercontainerInfoModels
                = appliercontainerService.findAllAppliercontainerInfoModels();

        Assert.assertEquals(13, appliercontainerInfoModels.size());
    }

    @Test
    public void TestAddAppliercontainerByInfoModel() {
        AppliercontainerInfoModel infoModel = new AppliercontainerInfoModel();
        infoModel.setDcName("fra").setAddr(new HostPort("1.1.1.1", 8080)).setActive(true).setAzName("A");

        Assert.assertNull(appliercontainerService.findByIpPort("1.1.1.1", 8080));
        appliercontainerService.addAppliercontainerByInfoModel(infoModel);

        Assert.assertNotNull(appliercontainerService.findByIpPort("1.1.1.1", 8080));

        try {
            appliercontainerService.addAppliercontainerByInfoModel(infoModel);
        } catch (Exception e) {
            Assert.assertEquals("Appliercontainer with IP: 1.1.1.1 already exists", e.getMessage());
        }

        infoModel.setAddr(new HostPort("2.2.2.2", 8080)).setDcName("none");
        try {
            appliercontainerService.addAppliercontainerByInfoModel(infoModel);
        } catch (Exception e) {
            Assert.assertEquals("dc name none dose not exist", e.getMessage());
        }

        infoModel.setAzName("AB").setDcName("jq");
        try {
            appliercontainerService.addAppliercontainerByInfoModel(infoModel);
        } catch (Exception e) {
            Assert.assertEquals("available zone AB does not exist", e.getMessage());
        }
    }

    @Test
    public void TestUpdateAppliercontainerByInfoModel() {
        AppliercontainerInfoModel infoModel = new AppliercontainerInfoModel();
        infoModel.setDcName("fra").setAddr(new HostPort("1.1.1.1", 8080)).setActive(true).setAzName("A");

        Assert.assertNull(appliercontainerService.findByIpPort("1.1.1.1", 8080));
        try {
            appliercontainerService.updateAppliercontainerByInfoModel(infoModel);
        } catch (Exception e) {
            Assert.assertEquals("Appliercontainer with IP: 1.1.1.1 does not exists", e.getMessage());
        }

        appliercontainerService.addAppliercontainerByInfoModel(infoModel);
        Assert.assertNotNull(appliercontainerService.findByIpPort("1.1.1.1", 8080));

        infoModel.setActive(false);
        appliercontainerService.updateAppliercontainerByInfoModel(infoModel);
        Assert.assertEquals(false,
                appliercontainerService.findByIpPort("1.1.1.1", 8080).isAppliercontainerActive());

        List<OrganizationTbl> allOrganizations = organizationService.getAllOrganizations();
        logger.info("all orgainizations : {}", allOrganizations);

        infoModel.setOrgName("org-1");
        appliercontainerService.updateAppliercontainerByInfoModel(infoModel);
        Assert.assertEquals(2,
                appliercontainerService.findByIpPort("1.1.1.1", 8080).getAppliercontainerOrg());

        infoModel.setAzName("AB");
        try {
            appliercontainerService.updateAppliercontainerByInfoModel(infoModel);
        } catch (Exception e) {
            Assert.assertEquals("available zone AB does not exist", e.getMessage());
        }
    }

    @Override
    protected String prepareDatas() throws IOException {
        return  prepareDatasFromFile("src/test/resources/applier-container-service-impl-test.sql");
    }
}
