package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblDao;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblEntity;
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
@Service("keepercontainerService")
public class KeepercontainerService {
    private KeepercontainerTblDao keepercontainerTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            keepercontainerTblDao= ContainerLoader.getDefaultContainer().lookup(KeepercontainerTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }

    public List<KeepercontainerTbl> findByDcName(String dcName) {
        try {
            return keepercontainerTblDao.findByDcName(dcName, KeepercontainerTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Metaservers not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load metaservers failed.", e);
        }
    }
}
