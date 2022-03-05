package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.unidal.tuple.Triple;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class DcMetaComparator extends AbstractMetaComparator<DcMeta, ClusterMeta, DcChange>{
	
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

	public DcMetaComparator() {
	}

	public DcMetaComparator(DcMeta current, DcMeta future) {
		super(current, future);
	}

	public void compare(){
		Map<Long, ClusterMeta> currentClustersMap = current.getClusters().values().stream()
				.collect(Collectors.toMap(ClusterMeta::getDbId, clusterMeta -> clusterMeta));
		Map<Long, ClusterMeta> futureClustersMap = future.getClusters().values().stream()
				.collect(Collectors.toMap(ClusterMeta::getDbId, clusterMeta -> clusterMeta));
		Triple<Set<Long>, Set<Long>, Set<Long>> result = getDiff(currentClustersMap.keySet(), futureClustersMap.keySet());

		Set<Long> addedClusterDbIds = result.getFirst();
		Set<Long> intersectionClusterDbIds = result.getMiddle();
		Set<Long> deletedClusterDbIds = result.getLast();
		
		for(Long clusterDbId : addedClusterDbIds){
			added.add(futureClustersMap.get(clusterDbId));
		}
		
		for(Long clusterDbId : deletedClusterDbIds){
			removed.add(currentClustersMap.get(clusterDbId));
		}
		
		for(Long clusterDbId : intersectionClusterDbIds){
			ClusterMeta currentMeta = currentClustersMap.get(clusterDbId);
			ClusterMeta futureMeta = futureClustersMap.get(clusterDbId);
			if(!reflectionEquals(currentMeta, futureMeta)) {
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
