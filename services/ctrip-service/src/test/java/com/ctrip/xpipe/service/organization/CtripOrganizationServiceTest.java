package com.ctrip.xpipe.service.organization;

import com.ctrip.xpipe.AbstractServiceTest;
import org.junit.Test;

/**
 * Created by zhuchen on 2017/8/30.
 */
public class CtripOrganizationServiceTest extends AbstractServiceTest{
    Organization organization;

    @Before
    public void beforeCtripOrganizationServiceTest() {
        organization = new CtripOrganizationService();
    }

    @Test
    public void testRetrieveOrganizationInfo() {
        List<OrganizationModel> organizationModelList = organization.retrieveOrganizationInfo();
        Assert.assertNotNull(organizationModelList);
        Assert.assertNotEquals(0, organizationModelList.size());
    }
}
