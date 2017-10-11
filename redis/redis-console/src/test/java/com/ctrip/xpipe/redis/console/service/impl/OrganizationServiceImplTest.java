package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.OrganizationDao;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;



/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */

public class OrganizationServiceImplTest extends AbstractServiceImplTest {

    @Autowired
    private OrganizationServiceImpl organizationService;

    @Autowired OrganizationDao organizationDao;

    @Test
    public void testGetOrgTblCreateListCase1() {
        List<OrganizationTbl> remoteDBOrgs = createOrganizationTblList(10);
        List<OrganizationTbl> localDBOrgs = createOrganizationTblList(5);
        List<OrganizationTbl> result = organizationService.getOrgTblCreateList(remoteDBOrgs, localDBOrgs);
        result.forEach(org->logger.info("org id: {}", org.getOrgId()));
        Assert.assertEquals(5, result.size());
    }

    @Test
    public void testGetOrgTblCreateListCase2() {
        List<OrganizationTbl> remoteDBOrgs = createOrganizationTblList(10);
        List<OrganizationTbl> localDBOrgs = createOrganizationTblList(11);
        List<OrganizationTbl> result = organizationService.getOrgTblCreateList(remoteDBOrgs, localDBOrgs);
        result.forEach(org->logger.info("org id: {}", org.getOrgId()));
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testGetOrgTblUpdateList() {
        List<OrganizationTbl> remoteDBOrgs = createOrganizationTblList(10)
            .stream().map(org->org.setOrgName(org.getOrgName() + "-suffix")).collect(Collectors.toList());
        List<OrganizationTbl> localDBOrgs = createOrganizationTblList(10);
        List<OrganizationTbl> result = organizationService.getOrgTblUpdateList(remoteDBOrgs, localDBOrgs);
        result.forEach(org->logger.info("org id: {}, org name: {}", org.getOrgId(), org.getOrgName()));
        Assert.assertEquals(10, result.size());
    }


    @Test
    public void testRetrieveOrgInfoFromRemote() throws InterruptedException {
        List<OrganizationTbl> orgs = organizationService.retrieveOrgInfoFromRemote();
        orgs.forEach(org->logger.info("{}", org));
        Assert.assertNotNull(orgs);
    }

    @Test
    public void testUpdateOrganizations() {
        organizationService.updateOrganizations();
        List<OrganizationTbl> orgs = organizationDao.findAllOrgs();
        orgs.forEach(org->logger.info("{}", org));
    }

    private List<OrganizationTbl> createOrganizationTblList(int count) {
        List<OrganizationTbl> result = new LinkedList<>();
        String prefix = "org-";
        for(int index = 1; index <= count; index++) {
            result.add(createOrganizationTbl(index, prefix + index));
        }
        return result;
    }

    private OrganizationTbl createOrganizationTbl(long id, String name) {
        return new OrganizationTbl().setOrgName(name).setOrgId(id);
    }
}
