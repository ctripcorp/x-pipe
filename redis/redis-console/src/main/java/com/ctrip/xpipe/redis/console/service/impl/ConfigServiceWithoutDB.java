package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.Date;
import java.util.List;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class ConfigServiceWithoutDB extends ConfigServiceImpl{

    private ConsolePortalService consolePortalService;

    @Autowired
    public ConfigServiceWithoutDB(ConsoleConfig consoleConfig, ConsolePortalService consolePortalService) {
        super(consoleConfig);
        this.consolePortalService = consolePortalService;
    }

    @Override
    protected void setConfig(ConfigModel config) throws DalException {
        consolePortalService.setConfig(config, null);
    }

    @Override
    protected ConfigTbl getConfigByKey(String key) throws DalException {
        return consolePortalService.getConfig(key, null);
    }

    @Override
    protected void setConfigAndUntil(ConfigModel config, Date date) throws DalException {
        consolePortalService.setConfig(config, date);
    }

    @Override
    protected ConfigTbl getByKeyAndSubId(String key, String subId) throws DalException {
        return consolePortalService.getConfig(key, subId);
    }

    @Override
    protected List<ConfigTbl> findAllByKeyAndValueAndUntilAfter(String key, String value, Date until) {
        return consolePortalService.findAllByKeyAndValueAndUntilAfter(key, value, until);
    }

    @Override
    protected List<ConfigTbl> getAllByKey(String key) throws DalException {
        return consolePortalService.getAllConfigs(key);
    }
}
