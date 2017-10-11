package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
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
public class OrganizationDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private OrganizationDao organizationDao;

    @Test
    public void testInsertBatch() {
        organizationDao.createBatchOrganizations(createBatchOrgTbl(5));
    }

    @Test
    public void testUpdateBatch() {
        organizationDao.createBatchOrganizations(createBatchOrgTbl(5));
        List<OrganizationTbl> orgs = organizationDao.findAllOrgs();
        List<OrganizationTbl> newOrgs = orgs.stream().map(org->{
            if(org.getOrgId() > 0) {
                org.setOrgId(org.getOrgId()).setOrgName(org.getOrgName()+"-update");
                return org;
            }
            return null;
        }).filter(org->org != null).collect(Collectors.toList());
        organizationDao.updateBatchOrganizations(newOrgs);
        organizationDao.findAllOrgs().forEach(org->logger.info("{}", org));
    }

    private OrganizationTbl createOrgTbl(long orgId, String orgName) {
        OrganizationTbl organizationTbl = new OrganizationTbl();
        organizationTbl.setOrgId(orgId).setOrgName(orgName);
        return organizationTbl;
    }

    private List<OrganizationTbl> createBatchOrgTbl(int count) {
        List<OrganizationTbl> result = new LinkedList<>();
        for(int i = 1; i <= count; i++) {
            result.add(createOrgTbl(i, "org-"+i));
        }
        return result;
    }

    @Test
    public void testFindByOrgId() {
        organizationDao.createBatchOrganizations(createBatchOrgTbl(5));
        OrganizationTbl organizationTbl = organizationDao.findByOrgId(2L);
        Assert.assertEquals(2L, organizationTbl.getOrgId());
    }
}
