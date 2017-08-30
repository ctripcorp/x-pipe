package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.OrganizationDao;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.AbstractOrganizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;


import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by zhuchen on 2017/8/29.
 */

//@Component("ctrip")
public class OrganizationServiceImpl extends AbstractOrganizationService {

    @Autowired
    private OrganizationDao organizationDao;

    @Override
    protected List<OrganizationTbl> retrieveOrgInfoFromRemote() {
//        RestTemplate restTemplate = new RestTemplate();
//        OrganizationTemplate organizationTemplate = restTemplate.postForObject(CMS_URL,
//            new CMSRequestBody(ACCESS_TOKEN), OrganizationTemplate.class);
//        List<OrganizationInfo> organizationInfos = organizationTemplate.getData();
//        return organizationInfos.stream()
//            .map(orgInfo->{
//                return new OrganizationTbl()
//                    .setOrgId(orgInfo.getOrganizationId())
//                    .setOrgName(orgInfo.getName());})
//            .collect(Collectors.toList());
        return null;
    }

    @Override
    protected List<OrganizationTbl> getOrgTblCreateList(List<OrganizationTbl> remoteDBOrgs,
        List<OrganizationTbl> localDBOrgs) {

        Set<Long> storedOrgId = new HashSet<>();
        localDBOrgs.forEach(org->storedOrgId.add(org.getOrgId()));
        return remoteDBOrgs.stream().filter(org->!storedOrgId.contains(org.getOrgId())).collect(Collectors.toList());
    }

    @Override
    protected List<OrganizationTbl> getOrgTblUpdateList(List<OrganizationTbl> remoteDBOrgs,
        List<OrganizationTbl> localDBOrgs) {

        Map<Long, OrganizationTbl> storedOrgTbl = new HashMap<>();
        localDBOrgs.forEach(org->storedOrgTbl.put(org.getOrgId(), org));
        return remoteDBOrgs.stream().filter(org->storedOrgTbl.containsKey(org.getOrgId())
            && org.getOrgName() != storedOrgTbl.get(org.getOrgId()).getOrgName()).collect(Collectors.toList());
    }

}
