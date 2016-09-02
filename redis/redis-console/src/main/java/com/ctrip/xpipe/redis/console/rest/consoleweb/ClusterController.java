package com.ctrip.xpipe.redis.console.rest.consoleweb;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author zhangle
 */
@RestController
@RequestMapping("console")
public class ClusterController extends AbstractConsoleController{
	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterService clusterService;
	
	@RequestMapping(value = "/clusters/{clusterName}/dcs", method = RequestMethod.GET)
	public List<DcTbl> findClusterDcs(@PathVariable String clusterName) {
		return valueOrEmptySet(DcTbl.class, dcService.findClusterRelatedDc(clusterName));
	}

	@RequestMapping(value = "/clusters/{clusterName}", method = RequestMethod.GET)
	public ClusterTbl loadCluster(@PathVariable String clusterName) {
		return valueOrDefault(ClusterTbl.class, clusterService.load(clusterName));
	}

	@RequestMapping(value = "/clusters/all", method = RequestMethod.GET)
	public List<ClusterTbl> findAllClusters() {
		return valueOrEmptySet(ClusterTbl.class, clusterService.findAllClusters());
	}
	
	@RequestMapping(value = "/count/clusters", method= RequestMethod.GET)
	public Long getClustersCount() {
		return valueOrDefault(Long.class, clusterService.getAllCount());
	}
	
	@RequestMapping(value = "/clusters", method = RequestMethod.POST)
	public ClusterTbl createCluster(@RequestBody ClusterTbl cluster) {
		return clusterService.createCluster(cluster);
	}

	@RequestMapping(value = "/clusters/{clusterName}", method = RequestMethod.PUT)
	public void updateCluster(@PathVariable String clusterName, @RequestBody ClusterTbl cluster) {
		clusterService.updateCluster(clusterName, cluster);
	}

	@RequestMapping(value = "/clusters/{clusterName}" , method = RequestMethod.DELETE)
	public void deleteCluster(@PathVariable String clusterName) {
		clusterService.deleteCluster(clusterName);
	}

	@RequestMapping(value = "/clusters/{clusterName}/dcs/{dcName}", method = RequestMethod.POST)
	public void bindDc(@PathVariable String clusterName, @PathVariable String dcName) {
		clusterService.bindDc(clusterName, dcName);
	}

	@RequestMapping(value = "/clusters/{clusterName}/dcs/{dcName}", method = RequestMethod.DELETE)
	public void unbindDc(@PathVariable String clusterName, @PathVariable String dcName) {
		clusterService.unbindDc(clusterName, dcName);
	}

}
