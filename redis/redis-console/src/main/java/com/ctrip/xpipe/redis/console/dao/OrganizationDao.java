package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTblDao;
import com.ctrip.xpipe.redis.console.model.OrganizationTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import jakarta.annotation.PostConstruct;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import java.util.List;

/**
 * Created by zhuchen on 2017/8/29.
 */

@Repository
public class OrganizationDao extends AbstractXpipeConsoleDAO {

    private OrganizationTblDao organizationTblDao;

    @Autowired
    private PlexusContainer container;

    @PostConstruct
    private void postConstruct() {
        try {
            organizationTblDao = container.lookup(OrganizationTblDao.class);
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
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return organizationTblDao.updateByPK(org, OrganizationTblEntity.UPDATESET_FULL);
            }
        });
    }

    @DalTransaction
    public void createBatchOrganizations(List<OrganizationTbl> orgs) {
        if (null != orgs) {
            queryHandler.handleBatchInsert(new DalQuery<int[]>() {
                @Override
                public int[] doQuery() throws DalException {
                    return organizationTblDao.insertBatch(orgs.toArray(new OrganizationTbl[orgs.size()]));
                }
            });
        }
    }

    @DalTransaction
    public void updateBatchOrganizations(List<OrganizationTbl> orgs) {
        if (null != orgs) {
            queryHandler.handleBatchUpdate(new DalQuery<int[]>() {
                @Override
                public int[] doQuery() throws DalException {
                    return organizationTblDao.updateBatch(orgs.toArray(new OrganizationTbl[orgs.size()]),
                        OrganizationTblEntity.UPDATESET_FULL);
                }
            });
        }
    }
}
