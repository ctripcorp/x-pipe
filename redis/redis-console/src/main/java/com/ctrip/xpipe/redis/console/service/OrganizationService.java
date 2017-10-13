package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.OrganizationTbl;

import java.util.List;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
public interface OrganizationService {
    void updateOrganizations();
    List<OrganizationTbl> getAllOrganizations();
    OrganizationTbl getOrganizationTblByCMSOrganiztionId(long organizationId);
}
