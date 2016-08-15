package com.ctrip.xpipe.redis.console.service;


import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.model.MetaserverTblDao;
import com.ctrip.xpipe.redis.console.model.MetaserverTblEntity;
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
@Service("metaserverService")
public class MetaserverService {
    private MetaserverTblDao metaserverTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            metaserverTblDao = ContainerLoader.getDefaultContainer().lookup(MetaserverTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }

    public List<MetaserverTbl> findByDcName(String dcName) {
        try {
            return metaserverTblDao.findByDcName(dcName, MetaserverTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Metaservers not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load metaservers failed.", e);
        }
    }
}
