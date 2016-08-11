package com.ctrip.xpipe.redis.console.rest.consoleweb;

import com.ctrip.xpipe.redis.console.entity.vo.ClusterVO;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.MetaInfoService;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.DalException;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * @author zhangle
 */
@RestController
@RequestMapping("console")
public class ClusterController {

    private List<ClusterTbl> clusters;
    @Autowired
    private MetaInfoService metaInfoService;

    @PostConstruct
    public void init() {
        clusters = new LinkedList<>();

        Random random = new Random();
        int size = random.nextInt(100) + 100;
        for (int i = 0; i < size; i++) {
            ClusterTbl c = new ClusterTbl();
            c.setActivedcId(random.nextInt(3));
            c.setClusterName("cluster" + i);
            c.setClusterDescription("desc" + random.nextDouble());
            clusters.add(c);
        }
    }

    @RequestMapping(value = "/clusters/{clusterName}/vo", method = RequestMethod.GET)
    public ClusterVO loadClustervo(@PathVariable String clusterName) throws DalException {

        return metaInfoService.getClusterVO(clusterName);
    }

    @RequestMapping("/clusters/{clusterName}/dcs")
    public List<DcTbl> findClusterDcs(String clusterName) {
        return null;
    }


    @RequestMapping(value = "/clusters/{clusterName}", method = RequestMethod.GET)
    public ClusterTbl loadCluster(@PathVariable String clusterName) {
        for (ClusterTbl clusterTbl : clusters) {
            if (clusterTbl.getClusterName().equals(clusterName)) {
                return clusterTbl;
            }
        }
        return null;
    }

    @RequestMapping(value = "/clusters/all", method = RequestMethod.GET)
    public List<ClusterTbl> findAllClusters() throws DalException {
        return clusters;
    }

    @RequestMapping(value = "/clusters", method = RequestMethod.GET)
    public List<ClusterTbl> findClusters(@RequestParam(defaultValue = "1") int page,
                                         @RequestParam(defaultValue = "50") int size) throws DalException {
        return null;
    }

    @RequestMapping(value = "/clusters", method = RequestMethod.POST)
    public ClusterTbl createCluster(@RequestBody ClusterTbl cluster) {
        clusters.add(cluster);
        return cluster;
    }

    /**
     * 1.先校验cluster是否存在,若不存在则抛异常
     * 2.activeDcId 不可修改
     *
     * @param clusterName
     * @param cluster
     */
    @RequestMapping(value = "/clusters/{clusterName}", method = RequestMethod.PUT)
    public void updateCluster(@PathVariable String clusterName, @RequestBody ClusterTbl cluster) {
        ClusterTbl clusterTbl = loadCluster(clusterName);
        clusterTbl.setClusterName(cluster.getClusterName());
        clusterTbl.setActivedcId(cluster.getActivedcId());
        clusterTbl.setClusterDescription(cluster.getClusterDescription());
    }

    @RequestMapping(value = "/clusters/{clusterName}", method = RequestMethod.DELETE)
    public void deleteCluster(@PathVariable String clusterName) {
        ClusterTbl clusterTbl = loadCluster(clusterName);
        clusters.remove(clusterTbl);
    }


}
