package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.ZoneDao;
import com.ctrip.xpipe.redis.console.model.ZoneTbl;
import com.ctrip.xpipe.redis.console.model.ZoneTblDao;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.ZoneService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author taotaotu
 * May 23, 2019
 */

@Service
public class ZoneServiceImpl extends AbstractConsoleService<ZoneTblDao> implements ZoneService {

    @Autowired
    private ZoneDao zoneDao;

    private ZoneTbl createZoneTbl(String zone_name){
        return dao.createLocal().setZoneName(zone_name);
    }

    @Override
    public ZoneTbl findById(long id) {
        return zoneDao.findById(id);
    }

    @Override
    public List<ZoneTbl> findAllZones() {
        return zoneDao.findAllZones();
    }

    @Override
    public synchronized void insertRecord(String zone_name) {

        zoneDao.insertRecord(createZoneTbl(zone_name));
    }
}
