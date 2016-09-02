package com.ctrip.xpipe.redis.core.meta.comparator;

import java.util.Set;

import org.unidal.tuple.Triple;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class DcMetaComparator extends AbstractMetaComparator<ClusterMeta>{
	
	private DcMeta current, future;
	
	
	public DcMetaComparator(DcMeta current, DcMeta future){
		this.current = current;
		this.future = future;
	}
	
	public void compare(){
		
		Triple<Set<String>, Set<String>, Set<String>> result = getDiff(current.getClusters().keySet(), future.getClusters().keySet());
		
		Set<String> addedClusterIds = result.getFirst(); 
		Set<String> intersectionClusterIds = result.getMiddle();
		Set<String> deletedClusterIds = result.getLast();
		
		for(String clusterId : addedClusterIds){
			added.add(future.findCluster(clusterId));
		}
		
		for(String clusterId : deletedClusterIds){
			removed.add(current.findCluster(clusterId));
		}
		
		for(String clusterId : intersectionClusterIds){
			ClusterMeta currentMeta = current.findCluster(clusterId);
			ClusterMeta futureMeta = future.findCluster(clusterId);
			if(!currentMeta.equals(futureMeta)){
				ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(currentMeta, futureMeta);
				clusterMetaComparator.compare();
				modified.add(clusterMetaComparator);
			}
		}
	}

}
