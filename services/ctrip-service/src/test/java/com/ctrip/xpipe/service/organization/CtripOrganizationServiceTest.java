package com.ctrip.xpipe.service.organization;

import com.ctrip.xpipe.service.AbstractServiceTest;
import com.ctrip.xpipe.api.organization.OrganizationModel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
public class CtripOrganizationServiceTest extends AbstractServiceTest{
    CtripOrganizationService organization;

    @Before
    public void beforeCtripOrganizationServiceTest() {
        organization = new CtripOrganizationService();
    }

    @Test
    public void testRetrieveOrganizationInfo() throws Exception {
        List<OrganizationModel> organizationModelList = organization.retrieveOrganizationInfo();
        Assert.assertNotNull(organizationModelList);
        Assert.assertNotEquals(0, organizationModelList.size());
    }
}
