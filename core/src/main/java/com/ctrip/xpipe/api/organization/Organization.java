package com.ctrip.xpipe.api.organization;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

import java.util.List;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
public interface Organization extends Ordered {
    public static Organization DEFAULT = ServicesUtil.getOrganizationService();

    List<OrganizationModel> retrieveOrganizationInfo() throws Exception;
}
