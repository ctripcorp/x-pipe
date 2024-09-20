package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.unidal.dal.jdbc.DalException;

import java.util.Date;
import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class ConfigUpdateController extends AbstractConsoleController {

    @Autowired
    private ConfigDao configDao;

    @RequestMapping(value = "/config/set", method = RequestMethod.POST)
    public RetMessage setConfig(@RequestParam ConfigModel config,
                                @RequestParam Date util) {
        try {
            if(util == null) {
                configDao.setConfig(config);
            } else {
                configDao.setConfigAndUntil(config, util);
            }
            return RetMessage.createSuccessMessage();
        } catch (DalException e) {
            logger.error("[setConfig] DalException: {}", e);
            return RetMessage.createFailMessage(e.getMessage());
        }

    }

    @GetMapping(value = "/config")
    public ConfigTbl getConfigSubId(@RequestParam String key,
                                    @RequestParam String subId) throws DalException {

        if(StringUtil.isEmpty(subId)) {
            return configDao.getByKey(key);
        } else {
            return configDao.getByKeyAndSubId(key, subId);
        }
    }

    @GetMapping(value = "/config/getAll/{key}")
    public List<ConfigTbl> getAllByKey(@PathVariable String key) throws DalException {
        return configDao.getAllByKey(key);
    }

    @RequestMapping(value = "/config/findAll", method = RequestMethod.POST)
    public List<ConfigTbl> findAllByKeyAndValueAndUntilAfter(@RequestParam String key,
                                                             @RequestParam String value,
                                                             @RequestParam Long until) throws DalException {

        return configDao.findAllByKeyAndValueAndUntilAfter(key, value, new Date(until));
    }

}
