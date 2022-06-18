package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.AppliercontainerCreateInfo;
import com.ctrip.xpipe.redis.console.model.AppliercontainerInfoModel;
import com.ctrip.xpipe.redis.console.model.AppliercontainerTbl;

import java.util.List;

public interface AppliercontainerService {
    AppliercontainerTbl findAppliercontainerTblById(long id);
    List<AppliercontainerTbl> findAllAppliercontainerTblsByDc(String dcName);
    List<AppliercontainerTbl> findAllActiveAppliercontainersByDc(String dcName);
    List<AppliercontainerTbl> findApplierCount(String dcName);
    List<AppliercontainerTbl> findBestAppliercontainersByDcCluster(String dcName, String clusterName);
    List<AppliercontainerTbl> findAllAppliercontainersByAz(long azId);
    AppliercontainerTbl findByIpPort(String ip, int port);

    List<AppliercontainerCreateInfo> findAllAppliercontainers();
    List<AppliercontainerCreateInfo> findAllAppliercontainerCreateInfosByDc(String dcName);
    void addAppliercontainerByCreateInfo(AppliercontainerCreateInfo createInfo);
    void updateAppliercontainerByCreateInfo(AppliercontainerCreateInfo createInfo);
    void deleteAppliercontainerByCreateInfo(String appliercontainerIp, int appliercontainerPort);

    List<AppliercontainerInfoModel> findAllAppliercontainerInfoModels();
    AppliercontainerInfoModel findAppliercontainerInfoModelById(long appliercontainerId);
    void addAppliercontainerByInfoModel(AppliercontainerInfoModel infoModel);
    void updateAppliercontainerByInfoModel(AppliercontainerInfoModel infoModel);

}
