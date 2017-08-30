package com.ctrip.xpipe.api.organization;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

import java.util.List;

/**
 * Created by zhuchen on 2017/8/30.
 */
public interface OrganizationService extends Ordered {
    OrganizationService DEFAULT = ServicesUtil.getOrganizationService();

    List<OrganizationModel> retrieveOrganizationInfo();
}
