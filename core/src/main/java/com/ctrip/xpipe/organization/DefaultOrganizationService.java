package com.ctrip.xpipe.organization;

import com.ctrip.xpipe.api.organization.OrganizationModel;
import com.ctrip.xpipe.api.organization.OrganizationService;

import java.util.List;

/**
 * Created by zhuchen on 2017/8/30.
 */
public class DefaultOrganizationService implements OrganizationService {

    @Override public List<OrganizationModel> retrieveOrganizationInfo() {
        return null;
    }

    @Override public int getOrder() {
        return 0;
    }
}
