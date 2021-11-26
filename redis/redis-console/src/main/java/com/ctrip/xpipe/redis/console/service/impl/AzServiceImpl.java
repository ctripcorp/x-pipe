package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.AzCreateInfo;
import com.ctrip.xpipe.redis.console.dao.AzDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.AzTbl;
import com.ctrip.xpipe.redis.console.model.AzTblDao;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.AzService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


/**
 * @author: Song_Yu
 * @date: 2021/11/8
 */
@Service
public class AzServiceImpl extends AbstractConsoleService<AzTblDao>
        implements AzService {

    @Autowired
    private DcService dcService;

    @Autowired
    private AzDao azDao;

    @Autowired
    private KeeperContainerServiceImpl keeperContainerService;

    @Override
    public void addAvailableZone(AzCreateInfo createInfo) {
        AzTbl proto = dao.createLocal();

        DcTbl dcTbl = dcService.find(createInfo.getDcName());
        if(null == dcTbl)
            throw new IllegalArgumentException(String.format("DC name %s does not exist", createInfo.getDcName()));

        if(availableZoneIsExist(createInfo))
            throw new IllegalArgumentException("available zone : " + createInfo.getAzName() + " already exists");

        if(null == createInfo.getDescription())
            createInfo.setDescription("");

        if(null == createInfo.isActive())
            createInfo.setActive(true);

        proto.setDcId(dcTbl.getId())
                .setActive(createInfo.isActive())
                .setAzName(createInfo.getAzName())
                .setDescription(createInfo.getDescription());
        azDao.addAvailablezone(proto);
    }

    @Override
    public void updateAvailableZone(AzCreateInfo createInfo) {
        AzTbl azTbl = getAvailableZoneTblByAzName(createInfo.getAzName());

        if(null == azTbl)
            throw new IllegalArgumentException(String.format("availablezone %s not found", createInfo.getAzName()));

        if(null != createInfo.isActive())
            azTbl.setActive(createInfo.isActive());

        if(null != createInfo.getDescription())
            azTbl.setDescription(createInfo.getDescription());

        azDao.updateAvailableZone(azTbl);
    }

    @Override
    public List<AzTbl> getDcActiveAvailableZoneTbls(String dcName) {
        DcTbl dcTbl = dcService.find(dcName);
        if(null == dcTbl)
            throw new IllegalArgumentException(String.format("DC name %s does not exist", dcName));

        return azDao.findActiveAvailableZonesByDc(dcTbl.getId());
    }

    @Override
    public List<AzTbl> getDcAvailableZoneTbls(String dcName) {
        DcTbl dcTbl = dcService.find(dcName);
        if(null == dcTbl)
            throw new IllegalArgumentException(String.format("DC name %s does not exist", dcName));

        return azDao.findAvailableZonesByDc(dcTbl.getId());
    }


    @Override
    public List<AzCreateInfo> getDcAvailableZoneInfos(String dcName) {

        return Lists.newArrayList(Lists.transform(getDcAvailableZoneTbls(dcName), new Function<AzTbl, AzCreateInfo>() {
            @Override
            public AzCreateInfo apply(AzTbl azTbl) {
                AzCreateInfo azCreateInfo = new AzCreateInfo()
                        .setDcName(dcName)
                        .setAzName(azTbl.getAzName())
                        .setActive(azTbl.isActive())
                        .setDescription(azTbl.getDescription());

                return azCreateInfo;
            }
        }));
    }

    @Override
    public List<AzCreateInfo> getAllAvailableZoneInfos() {
        List<AzTbl> azTbls = azDao.findAllAvailableZones();
        return Lists.newArrayList(Lists.transform(azTbls, new Function<AzTbl, AzCreateInfo>() {
            @Override
            public AzCreateInfo apply(AzTbl azTbl) {
                String dcName = dcService.getDcName(azTbl.getDcId());
                AzCreateInfo azCreateInfo = new AzCreateInfo()
                        .setDcName(dcName)
                        .setAzName(azTbl.getAzName())
                        .setActive(azTbl.isActive())
                        .setDescription(azTbl.getDescription());

                return azCreateInfo;
            }
        }));
    }

    @Override
    public void deleteAvailableZoneByName(String azName) {
        AzTbl azTbl = getAvailableZoneTblByAzName(azName);
        if(null == azTbl)
            throw new BadRequestException(String.format("availablezone %s not found", azName));

        List<KeepercontainerTbl> keepercontainerTbls = keeperContainerService.getKeeperContainerByAz(azTbl.getId());
        if(null != keepercontainerTbls && !keepercontainerTbls.isEmpty())
            throw new BadRequestException(String.format("This az %s is not empty, can not be deleted", azName));

        AzTbl proto = azTbl;
        azDao.deleteAvailableZone(proto);
    }


    @VisibleForTesting
    AzTbl getAvailableZoneTblByAzName(String azName) {
        return azDao.findAvailableZoneByAz(azName);
    }

    @VisibleForTesting
    boolean availableZoneIsExist(AzCreateInfo createInfo) {
        AzTbl exist = getAvailableZoneTblByAzName(createInfo.getAzName());
        return exist != null;
    }
}
