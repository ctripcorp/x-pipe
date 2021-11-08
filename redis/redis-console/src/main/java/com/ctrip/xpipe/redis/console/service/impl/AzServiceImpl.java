package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.AzCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.AzService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;


/**
 * @author:
 * @date:
 */
@Service
public class AzServiceImpl extends AbstractConsoleService<AzTblDao>
        implements AzService {

    @Autowired
    private DcService dcService;

    @Override
    public void addAvailableZone(AzCreateInfo createInfo) {
        AzTbl proto = dao.createLocal();

        DcTbl dcTbl = dcService.find(createInfo.getDcName());
        if(dcTbl == null) {
            throw new IllegalArgumentException(String.format("DC name %s does not exist", createInfo.getDcName()));
        }

        if(availableZoneIsExist(createInfo, dcTbl.getId())){
            throw new IllegalArgumentException("available zone : "
                    + createInfo.getAzName() + " already exists");
        }

        proto.setDcId(dcTbl.getId())
                .setActive(createInfo.isActive())
                .setAzName(createInfo.getAzName())
                .setDescription(createInfo.getDescription());

        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.insert(proto);
            }
        });
    }

    @Override
    public void updateAvailableZone(AzCreateInfo createInfo) {

        DcTbl dcTbl = dcService.find(createInfo.getDcName());
        if(dcTbl == null) {
            throw new IllegalArgumentException(String.format("DC name %s does not exist", createInfo.getDcName()));
        }

        AzTbl at = queryHandler.handleQuery(new DalQuery<AzTbl>() {
            @Override
            public AzTbl doQuery() throws DalException {
                return dao.findAvailableZoneByDcAndAz(dcTbl.getId(), createInfo.getAzName(), AzTblEntity.READSET_FULL);
            }
        });

        if(at == null){
            throw new IllegalArgumentException(String.format("availablezone %s not found", createInfo.getAzName()));
        }

        at.setDcId(dcTbl.getId())
                .setActive(createInfo.isActive())
                .setAzName(createInfo.getAzName())
                .setDescription(createInfo.getDescription());

        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.updateByPK(at, AzTblEntity.UPDATESET_FULL);
            }
        });
    }


    @Override
    public List<AzCreateInfo> getDcAvailableZones(String dcName) {
        //先获取dc——id
        DcTbl dcTbl = dcService.find(dcName);
        if(dcTbl == null) {
            throw new IllegalArgumentException(String.format("DC name %s does not exist", dcName));
        }
        //根据dc_id 查找
        List<AzTbl> azTbls = queryHandler.handleQuery(()->
                dao.findAvailableZoneByDc(dcTbl.getId(), AzTblEntity.READSET_FULL));


        return Lists.newArrayList(Lists.transform(azTbls, new Function<AzTbl, AzCreateInfo>() {
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
    public List<AzCreateInfo> getAllAvailableZones() {
        List<AzTbl> azTbls = queryHandler.handleQuery(()->
                dao.findAllAvailableZone(AzTblEntity.READSET_FULL));


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
    public void deleteAvailableZoneByName(String azName, String dcName) {
        AzTbl proto = null;

        proto =getAvailableZoneBydcAndAzName(azName,dcService.findByDcName(dcName).getId());
        if(proto == null) throw new BadRequestException(String.format("availablezone %s not found", azName));

        try {
            dao.deleteByPK(proto);
        } catch (DalException e) {
            throw new ServerException(e.getMessage());
        }
    }

    @Override
    public AzTbl getAzinfoByid(long id) {
        try {
            return dao.findByPK(id, AzTblEntity.READSET_FULL);
        } catch (DalException e) {
            e.printStackTrace();
            return null;
        }
    }

    AzTbl getAvailableZoneBydcAndAzName(String azName, long dcId){
        return queryHandler.handleQuery(new DalQuery<AzTbl>() {
            @Override
            public AzTbl doQuery() throws DalException {
                return dao.findAvailableZoneByDcAndAz(dcId, azName, AzTblEntity.READSET_FULL);
            }
        });
    }

    @VisibleForTesting
    boolean availableZoneIsExist(AzCreateInfo createInfo, Long dcId){
        AzTbl exist = queryHandler.handleQuery(new DalQuery<AzTbl>() {
            @Override
            public AzTbl doQuery() throws DalException {
              return dao.findAvailableZoneByDcAndAz(dcId, createInfo.getAzName(), AzTblEntity.READSET_FULL);
            }
        });
        return exist != null;
    }
}
