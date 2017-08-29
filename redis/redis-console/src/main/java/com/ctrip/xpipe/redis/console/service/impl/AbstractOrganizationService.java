package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.dao.OrganizationDao;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.util.List;

/**
 * Created by zhuchen on 2017/8/29.
 */
public abstract class AbstractOrganizationService extends AbstractConsoleService<OrganizationTbl>
    implements OrganizationService {

    @Autowired
    private OrganizationDao organizationDao;

    @Override
    public void updateOrganizations() {
        List<OrganizationTbl> remoteDBOrgs = retrieveOrgInfoFromRemote();
        List<OrganizationTbl> localDBOrgs = getAllOrganizations();
        List<OrganizationTbl> orgsToCreate = getOrgTblCreateList(remoteDBOrgs, localDBOrgs);
        List<OrganizationTbl> orgsToUpdate = getOrgTblUpdateList(remoteDBOrgs, localDBOrgs);
        updateDB(orgsToCreate, orgsToUpdate);
    }

    @DalTransaction
    protected void updateDB(List<OrganizationTbl> orgsToCreate, List<OrganizationTbl> orgsToUpdate) {
        if (null != orgsToCreate && orgsToCreate.size() > 0) {
            logger.info("[handleUpdate][orgsToCreate]{}, {}", orgsToUpdate.size(), orgsToUpdate);
            organizationDao.createBatchOrganizations(orgsToCreate);
        }

        if (null != orgsToUpdate && orgsToUpdate.size() > 0) {
            logger.info("[handleUpdate][orgsToUpdate]{}, {}", orgsToUpdate.size(), orgsToUpdate);
            organizationDao.updateBatchOrganizations(orgsToUpdate);
        }
    }

    @Override
    public List<OrganizationTbl> getAllOrganizations() {
        return organizationDao.findAllOrgs();
    }

    // Try to retrieve organization info from some source
    protected abstract List<OrganizationTbl> retrieveOrgInfoFromRemote();

    protected abstract List<OrganizationTbl> getOrgTblCreateList(List<OrganizationTbl> remoteDBOrgs,
                                                                    List<OrganizationTbl> localDBOrgs);

    protected abstract List<OrganizationTbl> getOrgTblUpdateList(List<OrganizationTbl> remoteDBOrgs,
                                                                    List<OrganizationTbl> localDBOrgs);
}
