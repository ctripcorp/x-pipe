package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.utils.StringUtil;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class OrganizationServiceWithoutDB implements OrganizationService {

    @Autowired
    private ConsolePortalService consolePortalService;

    @Autowired
    private ConsoleConfig config;

    private TimeBoundCache<List<OrganizationTbl>> allOrganizations;

    @PostConstruct
    public void postConstruct(){
        allOrganizations = new TimeBoundCache<>(config::getCacheRefreshInterval, consolePortalService::getAllOrganizations);
    }

    @Override
    public void updateOrganizations() {
        // not cross dc leader for disable database
        throw new UnsupportedOperationException();
    }

    @Override
    public List<OrganizationTbl> getAllOrganizations() {
        return allOrganizations.getData();
    }

    @Override
    public OrganizationTbl getOrganizationTblByCMSOrganiztionId(long organizationId) {
        List<OrganizationTbl> all = allOrganizations.getData();
        for(OrganizationTbl organizationTbl : all){
            if(organizationTbl.getOrgId() == organizationId){
                return organizationTbl;
            }
        }
        return null;
    }

    @Override
    public OrganizationTbl getOrgByName(String name) {
        List<OrganizationTbl> all = allOrganizations.getData();
        for(OrganizationTbl organizationTbl : all){
            if (StringUtil.trimEquals(organizationTbl.getOrgName(), name)) {
                return organizationTbl;
            }
        }
        return null;
    }

    @Override
    public List<OrganizationTbl> getInvolvedOrgs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OrganizationTbl getOrganization(long xpipeOrgId) {
        List<OrganizationTbl> all = allOrganizations.getData();
        for(OrganizationTbl organizationTbl : all){
            if (organizationTbl.getId() == xpipeOrgId) {
                return organizationTbl;
            }
        }
        return null;
    }
}
