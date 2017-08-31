package com.ctrip.xpipe.organization;

import com.ctrip.xpipe.api.organization.OrganizationModel;
import com.ctrip.xpipe.api.organization.Organization;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by zhuchen on 2017/8/30.
 */
public class DefaultOrganization implements Organization {

    @Override
    public List<OrganizationModel> retrieveOrganizationInfo() {
        List<OrganizationModel> result = new LinkedList<OrganizationModel>();
        OrganizationModel organizationModel = new OrganizationModel();
        organizationModel.setId(0L);
        organizationModel.setName("default");
        result.add(organizationModel);
        return result;
    }

    @Override public int getOrder() {
        return 0;
    }
}
