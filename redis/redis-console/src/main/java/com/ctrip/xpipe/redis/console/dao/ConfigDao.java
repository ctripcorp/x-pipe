package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.console.model.ConfigTblDao;
import com.ctrip.xpipe.redis.console.model.ConfigTblEntity;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.Date;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
@Repository
public class ConfigDao extends AbstractXpipeConsoleDAO{

    private ConfigTblDao configTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            configTblDao = ContainerLoader.getDefaultContainer().lookup(ConfigTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct dao.", e);
        }
    }

    public String getKey(String key) throws DalException {

        ConfigTbl byKey = configTblDao.findByKey(key, ConfigTblEntity.READSET_VALUE);
        return byKey.getValue();
    }

    protected ConfigTbl findByKey(long id) throws DalException {

        return configTblDao.findByPK(id, ConfigTblEntity.READSET_FULL);
    }

    public synchronized void setKey(String key, String val) throws DalException {
        ConfigModel model = new ConfigModel().setKey(key).setVal(val);
        setConfig(model);
    }

    public synchronized void setConfig(ConfigModel config) throws DalException {

        setConfig(config, null);
    }

    public synchronized void setConfigAndUntil(ConfigModel config, Date until) throws DalException {
        setConfig(config, until);
    }

    public void setConfig(ConfigModel config, Date until) throws DalException {
        logger.info("[setConfig] {}: {}", config, until);
        boolean insert = false;

        try{
            getKey(config.getKey());
        } catch (DalException e){
            logger.info("[setKey][not exist, create]{}", e.getMessage());
            insert = true;
        }

        ConfigTbl configTbl = new ConfigTbl();
        configTbl.setKey(config.getKey());
        configTbl.setValue(config.getVal());
        if(config.getUpdateIP() != null) {
            configTbl.setLatestUpdateIp(config.getUpdateIP());
        }
        if(config.getUpdateUser() != null) {
            configTbl.setLatestUpdateUser(config.getUpdateUser());
        }

        if(until != null) {
            configTbl.setUntil(until);
        }
        if(!insert) {
            configTblDao.updateValAndUntilByKey(configTbl, ConfigTblEntity.UPDATESET_FULL);
        }else{
            configTbl.setDesc("insert automatically");
            configTblDao.insert(configTbl);
        }
        logger.info("[setConfig] config update successfully, as {}", config.toString());
    }

    public ConfigTbl getByKey(String key) throws DalException {
        return configTblDao.findByKey(key, ConfigTblEntity.READSET_FULL);
    }

}
