package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTblDao;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by zhuchen on 2017/8/29.
 */
public class OrganizationInfoDaoTest extends AbstractConsoleIntegrationTest {
    @Autowired
    OrganizationDao organizationDao;

    @Test
    public void testInsertBatch() throws DalException {
        List<OrganizationTbl> orgs = createOrgList(3);
        organizationDao.createBatchOrganizations(orgs);
    }

    @Test
    public void testFindAllOrgs() throws DalException {
        organizationDao.createBatchOrganizations(createOrgList(5));
        List<OrganizationTbl> orgs = organizationDao.findAllOrgs();
        orgs.forEach(org->logger.info("org info: {}", org));
        Assert.assertEquals(6, orgs.size());
    }

    private List<OrganizationTbl> createOrgList(int count) {
        List<OrganizationTbl> result = new LinkedList<>();
        String prefix = "org-";
        for(int i = 1; i <= count; i++) {
            result.add(createOrg(i, prefix + i));
        }
        return result;
    }

    private OrganizationTbl createOrg(long orgId, String orgName) {
        OrganizationTbl proto = new OrganizationTbl()
                                    .setOrgId(orgId)
                                    .setOrgName(orgName);
        return proto;
    }
}
