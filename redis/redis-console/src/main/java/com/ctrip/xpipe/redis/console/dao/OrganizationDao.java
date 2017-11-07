package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTblDao;
import com.ctrip.xpipe.redis.console.model.OrganizationTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by zhuchen on 2017/8/29.
 */

@Repository
public class OrganizationDao extends AbstractXpipeConsoleDAO {

    private OrganizationTblDao organizationTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            organizationTblDao = ContainerLoader.getDefaultContainer().lookup(OrganizationTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct dao.", e);
        }
    }


    public OrganizationTbl findByPK(long id){
        return queryHandler.handleQuery(new DalQuery<OrganizationTbl>() {
            @Override
            public OrganizationTbl doQuery() throws DalException {
                return organizationTblDao.findByPK(id, OrganizationTblEntity.READSET_FULL);
            }
        });
    }

    public OrganizationTbl findByName(String orgName) {
        return queryHandler.handleQuery(new DalQuery<OrganizationTbl>() {
            @Override
            public OrganizationTbl doQuery() throws DalException {
                return organizationTblDao.findOrgByName(orgName, OrganizationTblEntity.READSET_FULL);
            }
        });
    }

    public OrganizationTbl findByOrgId(long orgId) {
        return queryHandler.handleQuery(new DalQuery<OrganizationTbl>() {
            @Override
            public OrganizationTbl doQuery() throws DalException {
                return organizationTblDao.findOrgByOrgId(orgId, OrganizationTblEntity.READSET_FULL);
            }
        });
    }

    public List<OrganizationTbl> findAllOrgs() {
        return queryHandler.handleQuery(new DalQuery<List<OrganizationTbl>>() {
            @Override
            public List<OrganizationTbl> doQuery() throws DalException {
                return organizationTblDao.findAllOrgs(OrganizationTblEntity.READSET_FULL);
            }
        });
    }

    public List<OrganizationTbl> findInvolvedOrgs() {
        return queryHandler.handleQuery(new DalQuery<List<OrganizationTbl>>() {
            @Override
            public List<OrganizationTbl> doQuery() throws DalException {
                return organizationTblDao.findInvolvedOrgs(OrganizationTblEntity.READSET_FULL);
            }
        });
    }

    @DalTransaction
    public void updateOrg(OrganizationTbl org) {
        queryHandler.handleQuery(new DalQuery<Void>() {
            @Override
            public Void doQuery() throws DalException {
                organizationTblDao.updateByPK(org, OrganizationTblEntity.UPDATESET_FULL);
                return null;
            }
        });
    }

    @DalTransaction
    public void createBatchOrganizations(List<OrganizationTbl> orgs) {
        if (null != orgs) {
            queryHandler.handleQuery(new DalQuery<Void>() {
                @Override
                public Void doQuery() throws DalException {
                    organizationTblDao.insertBatch(orgs.toArray(new OrganizationTbl[orgs.size()]));
                    return null;
                }
            });
        }
    }

    @DalTransaction
    public void updateBatchOrganizations(List<OrganizationTbl> orgs) {
        if (null != orgs) {
            queryHandler.handleQuery(new DalQuery<Void>() {
                @Override
                public Void doQuery() throws DalException {
                    organizationTblDao.updateBatch(orgs.toArray(new OrganizationTbl[orgs.size()]),
                        OrganizationTblEntity.UPDATESET_FULL);
                    return null;
                }
            });
        }
    }
}
