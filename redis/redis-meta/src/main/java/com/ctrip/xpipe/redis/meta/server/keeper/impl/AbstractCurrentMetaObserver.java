package com.ctrip.xpipe.redis.meta.server.keeper.impl;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.keeper.ClusterTypeAware;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author wenchao.meng
 *
 * Sep 4, 2016
 */
public abstract class AbstractCurrentMetaObserver extends AbstractLifecycleObservable implements Observer, ClusterTypeAware {
	
	@Autowired
	protected CurrentMetaManager currentMetaManager;
	
	@Autowired
	protected CurrentClusterServer currentClusterServer;
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		currentMetaManager.addObserver(this);
	}
	
	@Override
	protected void doDispose() throws Exception {
		
		currentMetaManager.removeObserver(this);
		super.doDispose();
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof NodeAdded){
			ClusterMeta clusterMeta = (ClusterMeta)((NodeAdded)args).getNode();
			if (supportCluster(clusterMeta)) {
				logger.info("[update][add][{}]{}", getClass().getSimpleName(), clusterMeta.getId());
				handleClusterAdd(clusterMeta);
			}
			return;
		}
		
		if(args instanceof NodeDeleted){
			ClusterMeta clusterMeta = (ClusterMeta)((NodeDeleted)args).getNode();
			if (supportCluster(clusterMeta)) {
				logger.info("[update][delete][{}]{}", getClass().getSimpleName(), clusterMeta.getId());
				handleClusterDeleted(clusterMeta);
			}
			return;
		}
		
		if(args instanceof ClusterMetaComparator){
			ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator)args;
			if (supportCluster(clusterMetaComparator.getCurrent())) {
				logger.info("[update][modify][{}]{}", getClass().getSimpleName(), clusterMetaComparator);
				handleClusterModified(clusterMetaComparator);
			}
			return;
		}
		
		throw new IllegalArgumentException("unknown argument:" + args);
	}

	protected boolean registerJob(Long clusterDbId, Long shardDbId, Releasable releasable) {
		try {
			currentMetaManager.addResource(clusterDbId, shardDbId, releasable);
		} catch (Exception e) {
			try {
				logger.info("[registerJob][{}][{}] cancel job registration", clusterDbId, shardDbId, e);
				releasable.release();
			} catch (Throwable t) {
				logger.warn("[registerJob][{}][{}]", clusterDbId, shardDbId, t);
			}
			return false;
		}
		return true;
	}

	protected boolean supportCluster(ClusterMeta clusterMeta) {
		return !StringUtil.isEmpty(clusterMeta.getType())
				&& getSupportClusterTypes().contains(ClusterType.lookup(clusterMeta.getType()));
	}

	protected abstract void handleClusterModified(ClusterMetaComparator comparator);

	protected abstract void handleClusterDeleted(ClusterMeta clusterMeta);

	protected abstract void handleClusterAdd(ClusterMeta clusterMeta);

	public void setCurrentMetaManager(CurrentMetaManager currentMetaManager) {
		this.currentMetaManager = currentMetaManager;
	}
	
	@Override
	public int getOrder() {
		return CurrentClusterServer.ORDER + 1;
	}
}
