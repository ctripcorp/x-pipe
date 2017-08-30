package com.ctrip.xpipe.api.organization;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.organization.OrganizationModel;
import com.ctrip.xpipe.api.organization.OrganizationService;

import java.util.List;

/**
 * Created by zhuchen on 2017/8/30.
 */
public class CtripOrganizationService implements OrganizationService {

    private Config config = Config.DEFAULT;

    @Override
    public List<OrganizationModel> retrieveOrganizationInfo() {
        String accessToken = config.getCmsAccessToken();
        
    }
}
