package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */

@Repository
public class ProxyDao extends AbstractXpipeConsoleDAO {

    private ProxyTblDao dao;

    @PostConstruct
    private void postConstruct() {
        try {
            dao = ContainerLoader.getDefaultContainer().lookup(ProxyTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct dao.", e);
        }
    }

    public List<ProxyTbl> getAllProxyTbls() {
        return queryHandler.handleQuery(new DalQuery<List<ProxyTbl>>() {
            @Override
            public List<ProxyTbl> doQuery() throws DalException {
                return dao.findAllActive(ProxyTblEntity.READSET_ID_DC_URI);
            }
        });
    }

    public void insert(ProxyTbl proto) {
        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.insert(proto);
            }
        });
    }
}
