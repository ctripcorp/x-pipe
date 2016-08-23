package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.DcTblEntity;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.List;

@Service
public class DcService {
    private DcTblDao dcTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            dcTblDao = ContainerLoader.getDefaultContainer().lookup(DcTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }


    public DcTbl load(String dcName) {
        try {
            return dcTblDao.findDcByDcName(dcName, DcTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Dc not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load dc failed.", e);
        }
    }

    public DcTbl load(long dcId) {
        try {
            return dcTblDao.findByPK(dcId, DcTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Dc not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load dc failed.", e);
        }
    }

    public List<DcTbl> findAllDcs() {
        try {
            return dcTblDao.findAllDcs(DcTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Dc not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load all dc failed.", e);
        }
    }

    public List<DcTbl> findClusterRelatedDc(String clusterName) {
        try {
            return dcTblDao.findClusterRelatedDc(clusterName, DcTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Cluster related dc not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load cluster related dc failed.", e);
        }
    }

}
