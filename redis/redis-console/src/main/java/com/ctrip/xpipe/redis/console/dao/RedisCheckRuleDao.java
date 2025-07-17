package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.RedisCheckRuleTbl;
import com.ctrip.xpipe.redis.console.model.RedisCheckRuleTblDao;
import com.ctrip.xpipe.redis.console.model.RedisCheckRuleTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import jakarta.annotation.PostConstruct;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import java.util.List;

@Repository
public class RedisCheckRuleDao extends AbstractXpipeConsoleDAO{

    private RedisCheckRuleTblDao redisCheckRuleTblDao;

    @Autowired
    private PlexusContainer plexusContainer;

    @PostConstruct
    private void postConstruct() {
        try {
            redisCheckRuleTblDao = plexusContainer.lookup(RedisCheckRuleTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct azTblDao.", e);
        }
    }

    public void addRedisCheckRule(RedisCheckRuleTbl proto) {
        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return redisCheckRuleTblDao.insert(proto);
            }
        });

    }

    public void updateRedisCheckRule(RedisCheckRuleTbl proto) {
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return redisCheckRuleTblDao.updateByPK(proto, RedisCheckRuleTblEntity.UPDATESET_FULL);
            }
        });
    }

    public void deleteRedisCheckRule(final RedisCheckRuleTbl redisCheckRuleTbl) {
        RedisCheckRuleTbl proto = redisCheckRuleTbl;
        proto.setParam(generateDeletedName(redisCheckRuleTbl.getParam()));

        queryHandler.handleDelete(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return redisCheckRuleTblDao.deleteRedisCheckRule(proto, RedisCheckRuleTblEntity.UPDATESET_FULL);
            }
        }, true);
    }

    public RedisCheckRuleTbl getRedisCheckRuleById(Long ruleId) {
        return  queryHandler.handleQuery(new DalQuery<RedisCheckRuleTbl>() {
            @Override
            public RedisCheckRuleTbl doQuery() throws DalException {
                return redisCheckRuleTblDao.findRedisCheckRuleById(ruleId, RedisCheckRuleTblEntity.READSET_FULL);
            }
        });
    }

    public RedisCheckRuleTbl getRedisCheckRuleByParam(String param) {
        return queryHandler.handleQuery(new DalQuery<RedisCheckRuleTbl>() {
            @Override
            public RedisCheckRuleTbl doQuery() throws DalException {
                return redisCheckRuleTblDao.findRedisCheckRulesByParam(param, RedisCheckRuleTblEntity.READSET_FULL);
            }
        });
    }

    public List<RedisCheckRuleTbl> getRedisCheckRulesByCheckType(String checkType) {
        return  queryHandler.handleQuery(new DalQuery<List<RedisCheckRuleTbl>>() {
            @Override
            public List<RedisCheckRuleTbl> doQuery() throws DalException {
                return redisCheckRuleTblDao.findRedisCheckRulesByCheckerType(checkType, RedisCheckRuleTblEntity.READSET_FULL);
            }
        });
    }

    public List<RedisCheckRuleTbl> getAllRedisCheckRules() {
        return queryHandler.handleQuery(new DalQuery<List<RedisCheckRuleTbl>>() {
            @Override
            public List<RedisCheckRuleTbl> doQuery() throws DalException {
                return redisCheckRuleTblDao.findAllRedisCheckRules(RedisCheckRuleTblEntity.READSET_FULL);
            }
        });
    }

}
