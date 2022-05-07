package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.RouteDao;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteDirectionModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.service.RouteService;
import com.ctrip.xpipe.redis.core.util.OrgUtil;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
@Service
public class RouteServiceImpl implements RouteService {

    @Autowired
    private RouteDao routeDao;

    @Autowired
    private ClusterDao clusterDao;

    @Autowired
    private DcService dcService;

    @Autowired
    private OrganizationService organizationService;

    @Autowired
    private ProxyService proxyService;

    @Override
    public List<RouteTbl> getActiveRouteTbls() {
        return routeDao.getAllActiveRoutes();
    }

    @Override
    public List<RouteModel> getAllRoutes() {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        List<RouteModel> clone = Lists.transform(routeDao.getAllRoutes(), new Function<RouteTbl, RouteModel>() {
            @Override
            public RouteModel apply(RouteTbl input) {
                return RouteModel.fromRouteTbl(input, mapper);
            }
        });
        return Lists.newArrayList(clone);
    }

    @Override
    public List<RouteModel> getActiveRoutes() {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        List<RouteModel> clone = Lists.transform(routeDao.getAllActiveRoutes(), new Function<RouteTbl, RouteModel>() {
            @Override
            public RouteModel apply(RouteTbl input) {
                return RouteModel.fromRouteTbl(input, mapper);
            }
        });
        return Lists.newArrayList(clone);
    }

    @Override
    public RouteInfoModel getRouteInfoModelById(long routeId) {
        return convertRouteTblToRouteInfoModel(routeDao.getRouteById(routeId),
                new DcIdNameMapper.DefaultMapper(dcService), proxyService.proxyIdUriMap());
    }

    @Override
    public List<RouteInfoModel> getAllActiveRouteInfoModels() {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        Map<Long, String> proxyIdUriMap = proxyService.proxyIdUriMap();

        List<RouteInfoModel> clone = Lists.transform(routeDao.getAllActiveRoutes(), new Function<RouteTbl, RouteInfoModel>() {
            @Override
            public RouteInfoModel apply(RouteTbl routeTbl) {
                return convertRouteTblToRouteInfoModel(routeTbl, mapper, proxyIdUriMap);
            }
        });

        return Lists.newArrayList(clone);
    }

    @Override
    public Map<Long, RouteInfoModel> getRouteIdInfoModelMap(){
        Map<Long, RouteInfoModel> result = new HashMap<>();
        getAllActiveRouteInfoModels().forEach(routeInfoModel -> result.put(routeInfoModel.getId(), routeInfoModel));

        return result;
    }

    @Override
    public List<RouteInfoModel> getAllActiveRouteInfoModelsByTagAndSrcDcName(String tag, String srcDcName) {
        DcTbl srcDcTbl = dcService.findByDcName(srcDcName);
        return convertRouteTblsToRouteInfoModels(routeDao.getAllActiveRoutesByTagAndSrcDcId(tag, srcDcTbl.getId()));
    }

    @Override
    public List<RouteInfoModel> getAllActiveRouteInfoModelsByTag(String tag) {
        return convertRouteTblsToRouteInfoModels(routeDao.getAllActiveRoutesByTag(tag));
    }

    @Override
    public List<RouteInfoModel> getAllActiveRouteInfoModelsByTagAndDirection(String tag, String srcDcName, String dstDcName) {
        DcTbl srcDcTbl = dcService.findByDcName(srcDcName);
        DcTbl dstDcTbl = dcService.findByDcName(dstDcName);

        return convertRouteTblsToRouteInfoModels(routeDao.getAllActiveRoutesByTagAndDirection(tag, srcDcTbl.getId(), dstDcTbl.getId()));
    }

    private List<RouteInfoModel> convertRouteTblsToRouteInfoModels(List<RouteTbl> routeTbls) {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        Map<Long, String> proxyIdUriMap = proxyService.proxyIdUriMap();

        List<RouteInfoModel> clone = Lists.transform(routeTbls, new Function<RouteTbl, RouteInfoModel>() {
            @Override
            public RouteInfoModel apply(RouteTbl routeTbl) {
                return convertRouteTblToRouteInfoModel(routeTbl, mapper, proxyIdUriMap);
            }
        });

        return Lists.newArrayList(clone);
    }

    @Override
    public RouteInfoModel convertRouteTblToRouteInfoModel(RouteTbl routeTbl, DcIdNameMapper dcIdNameMapper, Map<Long, String> proxyIdUriMap) {
        RouteInfoModel routeInfoModel = new RouteInfoModel();
        if(routeTbl == null) return routeInfoModel;

        routeInfoModel.setActive(routeTbl.isActive()).setId(routeTbl.getId()).setTag(routeTbl.getTag())
                .setSrcDcName(dcIdNameMapper.getName(routeTbl.getSrcDcId()))
                .setSrcProxies(getProxyUriByIds(routeTbl.getSrcProxyIds(), proxyIdUriMap))
                .setOptionalProxies(getProxyUriByIds(routeTbl.getOptionalProxyIds(), proxyIdUriMap))
                .setDstDcName(dcIdNameMapper.getName(routeTbl.getDstDcId()))
                .setDstProxies(getProxyUriByIds(routeTbl.getDstProxyIds(), proxyIdUriMap))
                .setPublic(routeTbl.isIsPublic())
                .setDescription(routeTbl.getDescription());

        if (!OrgUtil.isDefaultOrg(routeTbl.getRouteOrgId())) {
            routeInfoModel.setOrgName(organizationService.getOrganizationTblByCMSOrganiztionId(routeTbl.getRouteOrgId()).getOrgName());
        }
        return routeInfoModel;
    }

    private List<String> getProxyUriByIds(String proxyIds, Map<Long, String> proxyIdUriMap) {
        if(proxyIds.isEmpty()) return Collections.emptyList();
        Set<String> proxys = Sets.newHashSet(proxyIds.split(","));
        List<String> proxyUris = new ArrayList<>();

        proxys.forEach((proxyId) -> {
            proxyUris.add(proxyIdUriMap.get(Long.parseLong(proxyId)));
        });

        return proxyUris;
    }

    @Override
    public List<RouteDirectionModel> getAllRouteDirectionModelsByTag(String tag) {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        Map<Long, String> proxyIdUriMap = proxyService.proxyIdUriMap();

        Map<SrcDest, RouteDirectionModel> result = new HashMap<>();
        List<RouteTbl> allRoutesByTag = routeDao.getAllActiveRoutesByTag(tag);
        allRoutesByTag.forEach(routeTbl -> {
            String srcDcName = mapper.getName(routeTbl.getSrcDcId());
            String dstDcName = mapper.getName(routeTbl.getDstDcId());

            RouteDirectionModel routeDirectionModel = MapUtils.getOrCreate(result, new SrcDest(srcDcName, dstDcName), () -> {
                return new RouteDirectionModel(srcDcName, dstDcName);
            });
            routeDirectionModel.getRoutes().add(convertRouteTblToRouteInfoModel(routeTbl, mapper,  proxyIdUriMap));
            routeDirectionModel.activeRouteNumIncrement();
            if(routeTbl.isIsPublic()) routeDirectionModel.publicRouteNumIncrement();
        });

        return Lists.newArrayList(result.values());
    }

    @Override
    public void updateRoute(RouteModel model) {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        routeDao.update(model.toRouteTbl(mapper));
    }

    @Override
    public void updateRoute(RouteInfoModel model) {
        routeDao.update(convertRouteInfoModelToRouteTbl(model));
    }

    @Override
    @DalTransaction
    public void updateRoutes(List<RouteInfoModel>models) {
        models.forEach(model -> updateRoute(model));
    }

    @Override
    public void deleteRoute(long id) {
        routeDao.delete(id);
    }

    @Override
    public void addRoute(RouteModel model) {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        routeDao.insert(model.toRouteTbl(mapper));
    }

    @Override
    public void addRoute(RouteInfoModel model) {
        routeDao.insert(convertRouteInfoModelToRouteTbl(model));
    }

    private RouteTbl convertRouteInfoModelToRouteTbl(RouteInfoModel model) {

        RouteTbl routeTbl = new RouteTbl();
        Map<String, Long> proxyUriIdMap = proxyService.proxyUriIdMap();

        if(model.getId() != 0)  routeTbl.setId(model.getId());
        if(model.getOrgName() != null) routeTbl.setRouteOrgId(organizationService.getOrgByName(model.getOrgName()).getOrgId());

        routeTbl.setSrcProxyIds(model.getSrcProxies() == null ? "" : StringUtil.join(",", (arg) -> proxyUriIdMap.get(arg).toString(), model.getSrcProxies()));
        routeTbl.setOptionalProxyIds(model.getOptionalProxies() == null ? "" : StringUtil.join(",", (arg) -> proxyUriIdMap.get(arg).toString(), model.getOptionalProxies()));
        routeTbl.setDstProxyIds(model.getDstProxies() == null ? "" : StringUtil.join(",", (arg) -> proxyUriIdMap.get(arg).toString(), model.getDstProxies()));

        routeTbl.setActive(model.isActive()).setTag(model.getTag()).setIsPublic(model.isPublic())
                .setSrcDcId(dcService.findByDcName(model.getSrcDcName()).getId())
                .setDstDcId(dcService.findByDcName(model.getDstDcName()).getId())
                .setDescription(model.getDescription());

        return routeTbl;
    }

    private String getProxyIdsByUris(List<String> proxyUris, Map<String, Long> proxyUriIdMap) {
        return StringUtil.join(",", (arg) -> proxyUriIdMap.get(arg).toString(), proxyUris);
    }

    @Override
    public boolean existsRouteBetweenDc(String activeDc, String backupDc) {
        List<RouteModel> routes = getActiveRoutes();
        for(RouteModel route : routes) {
            if(route.getSrcDcName().equalsIgnoreCase(backupDc)
                    && route.getDstDcName().equalsIgnoreCase(activeDc))
                return true;
        }
        return false;
    }

    @Override
    public boolean existPeerRoutes(String currentDc, String clusterName) {
        List<String> peerDcs = new ArrayList<String>();
        for (DcTbl relatedDc : dcService.findClusterRelatedDc(clusterName)) {
            peerDcs.add(relatedDc.getDcName());
        }

        List<RouteModel> routes = getActiveRoutes();
        for (String peerDc: peerDcs) {
            if (peerDc.equals(currentDc)) continue;
            for(RouteModel route : routes) {
                if(route.getSrcDcName().equalsIgnoreCase(currentDc)
                        && route.getDstDcName().equalsIgnoreCase(peerDc))
                    return true;
            }
        }

        return false;
    }

    private static class SrcDest extends Pair<String, String> {
        public SrcDest(String key, String value) {
            super(key, value);
        }
    }
}
