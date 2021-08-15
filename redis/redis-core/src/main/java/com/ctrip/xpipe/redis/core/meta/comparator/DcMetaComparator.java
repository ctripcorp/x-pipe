package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.unidal.tuple.Triple;

import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class DcMetaComparator extends AbstractMetaComparator<ClusterMeta, DcChange>{
	
	private DcMeta current, future;
	
	
	public static DcMetaComparator  buildComparator(DcMeta current, DcMeta future){
		
		DcMetaComparator dcMetaComparator = new DcMetaComparator(current, future);
		dcMetaComparator.compare();
		return dcMetaComparator;
	}
	
	public static DcMetaComparator buildClusterRemoved(ClusterMeta clusterMeta){
		DcMetaComparator dcMetaComparator = new DcMetaComparator();
		dcMetaComparator.removed.add(clusterMeta);
		return dcMetaComparator;
	}

	public static DcMetaComparator buildClusterChanged(ClusterMeta current, ClusterMeta future){
		
		DcMetaComparator dcMetaComparator = new DcMetaComparator();
		if(current == null){
			dcMetaComparator.added.add(future);
			return dcMetaComparator;
		}
		
		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();
		dcMetaComparator.modified.add(clusterMetaComparator);
		return dcMetaComparator;
	}

	private DcMetaComparator(){
		
	}
	
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
			if(!currentMeta.getType().equals(futureMeta.getType())) {
				removed.add(currentMeta);
				added.add(futureMeta);
			}
			if(!reflectionEquals(currentMeta, futureMeta)){
				ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(currentMeta, futureMeta);
				clusterMetaComparator.compare();
				modified.add(clusterMetaComparator);
			}
		}
	}

	@Override
	public String idDesc() {

		if(current != null){
			return current.getId();
		}
		if(future != null){
			return future.getId();
		}
		return null;
	}
}
