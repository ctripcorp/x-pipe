package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.OrganizationTbl;

import java.util.List;

/**
 * Created by zhuchen on 2017/8/29.
 */
public interface OrganizationService {
    void updateOrganizations();
    List<OrganizationTbl> getAllOrganizations();
    OrganizationTbl getOrganizationTblByCMSOrganiztionId(long organizationId);
}
