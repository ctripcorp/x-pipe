package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcModel;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.DcListDcModel;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;

import static com.ctrip.xpipe.redis.console.service.impl.DcServiceImpl.convertDcTblToDcModel;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class DcServiceWithoutDB implements DcService {

    @Autowired
    private ConsolePortalService consolePortalService;

    @Autowired
    private ConsoleConfig config;

    private TimeBoundCache<List<DcTbl>> dcs;

    @PostConstruct
    public void postConstruct() {
        dcs = new TimeBoundCache<>(config::getCacheRefreshInterval, consolePortalService::getAllDcs);
    }

    @Override
    public DcTbl find(String dcName) {
        List<DcTbl> dcs = findAllDcs();
        for (DcTbl dc : dcs) {
            if (StringUtil.trimEquals(dc.getDcName(), dcName)) {
                return dc;
            }
        }
        return null;
    }

    @Override
    public DcTbl find(long dcId) {
        List<DcTbl> dcs = findAllDcs();
        for (DcTbl dc : dcs) {
            if (dc.getId() == dcId) {
                return dc;
            }
        }
        return null;
    }

    @Override
    public String getDcName(long dcId) {
        DcTbl dc = find(dcId);
        if(dc != null) {
            return dc.getDcName();
        }
        return "";
    }

    @Override
    public List<DcTbl> findAllDcs() {
        return dcs.getData();
    }

    @Override
    public List<String> findAllDcNames() {
        List<DcTbl> all = findAllDcs();
        List<String> result = new ArrayList<>();
        for (DcTbl dc : all) {
            result.add(dc.getDcName());
        }
        return result;
    }

    @Override
    public List<DcTbl> findAllDcBasic() {
        return findAllDcs();
    }

    @Override
    public List<DcTbl> findClusterRelatedDc(String clusterName) {
        return consolePortalService.findClusterRelatedDc(clusterName);
    }

    @Override
    public DcTbl findByDcName(String activeDcName) {
        List<DcTbl> all = findAllDcs();
        for (DcTbl dc : all) {
            if (StringUtil.trimEquals(dc.getDcName(), activeDcName)) {
                return dc;
            }
        }
        return null;
    }

    @Override
    public Map<Long, String> dcNameMap() {
        List<DcTbl> allDcs = findAllDcs();
        Map<Long, String> result = new HashMap<>();
        allDcs.forEach(dcTbl -> result.put(dcTbl.getId(), dcTbl.getDcName()));
        return result;
    }

    @Override
    public Map<String, Long> dcNameIdMap() {
        List<DcTbl> allDcs = findAllDcs();
        Map<String, Long> result = new HashMap<>();
        allDcs.forEach(dcTbl -> result.put(dcTbl.getDcName(), dcTbl.getId()));
        return result;
    }

    @Override
    public Map<String, Long> dcNameZoneMap() {
        List<DcTbl> allDcs = findAllDcs();
        Map<String, Long> result = new HashMap<>();
        allDcs.forEach(dcTbl -> result.put(dcTbl.getDcName(), dcTbl.getZoneId()));
        return result;
    }

    @Override
    public List<DcListDcModel> findAllDcsRichInfo(boolean isCountTypeInHetero) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertWithPartField(long zoneId, String dcName, String description) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DcModel findDcModelByDcName(String dcName) {
        return convertDcTblToDcModel(find(dcName));
    }

    @Override
    public DcModel findDcModelByDcId(long dcId) {
        return convertDcTblToDcModel(find(dcId));
    }

    @Override
    public void updateDcZone(DcModel dcModel) {
        throw new UnsupportedOperationException();
    }
}
