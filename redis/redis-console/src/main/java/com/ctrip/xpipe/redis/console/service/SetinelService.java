package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTblDao;
import com.ctrip.xpipe.redis.console.model.SetinelTblEntity;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by Chris on 8/20/16.
 */
@Service("setinelService")
public class SetinelService {
    private SetinelTblDao setinelTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            setinelTblDao = ContainerLoader.getDefaultContainer().lookup(SetinelTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }

    public List<SetinelTbl> findByDcName(String dcName) {
        try {
           return setinelTblDao.findByDcName(dcName, SetinelTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Metaservers not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load metaservers failed.", e);
        }
    }
}
