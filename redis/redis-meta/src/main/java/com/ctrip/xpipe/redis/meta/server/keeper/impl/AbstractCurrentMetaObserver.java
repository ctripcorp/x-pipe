package com.ctrip.xpipe.redis.meta.server.keeper.impl;

import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;

/**
 * @author wenchao.meng
 *
 * Sep 4, 2016
 */
public abstract class AbstractCurrentMetaObserver extends AbstractLifecycleObservable implements Observer{
	
	@Autowired
	protected CurrentMetaManager currentMetaManager;
	
	@Autowired
	protected CurrentClusterServer currentClusterServer; 
	
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		currentMetaManager.addObserver(this);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof NodeAdded){
			ClusterMeta clusterMeta = (ClusterMeta)((NodeAdded)args).getNode();
			logger.info("[update][add]{}", clusterMeta);
			handleClusterAdd(clusterMeta);
		}
		
		if(args instanceof NodeDeleted){
			ClusterMeta clusterMeta = (ClusterMeta)((NodeDeleted)args).getNode();
			logger.info("[update][delete]{}", clusterMeta);
			handleClusterDeleted(clusterMeta);
		}
		
		if(args instanceof ClusterMetaComparator){
			ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator)args;
			logger.info("[update][modify]{}", clusterMetaComparator);
			handleClusterModified(clusterMetaComparator);
		}
		
		throw new IllegalArgumentException("unknown argument:" + args);
	}

	protected abstract void handleClusterModified(ClusterMetaComparator comparator);

	protected abstract void handleClusterDeleted(ClusterMeta clusterMeta);

	protected abstract void handleClusterAdd(ClusterMeta clusterMeta);

}
