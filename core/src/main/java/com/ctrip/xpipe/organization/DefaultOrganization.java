package com.ctrip.xpipe.organization;

import com.ctrip.xpipe.api.organization.OrganizationModel;
import com.ctrip.xpipe.api.organization.Organization;

import java.util.LinkedList;
import java.util.List;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
public class DefaultOrganization implements Organization {

    @Override
    public List<OrganizationModel> retrieveOrganizationInfo() {
        int count = 10;
        return createOrganizationModelList(count);
    }

    private List<OrganizationModel> createOrganizationModelList(int count) {
        List<OrganizationModel> result = new LinkedList<>();
        String prefix = "org-model-";
        for(int i = 1; i <= count; i++) {
            result.add(createOrganizationModel(i, prefix + i));
        }
        return result;
    }

    private OrganizationModel createOrganizationModel(long id, String name) {
        OrganizationModel organizationModel = new OrganizationModel();
        organizationModel.setId(id);
        organizationModel.setName(name);
        return organizationModel;
    }

    @Override public int getOrder() {
        return 0;
    }
}
