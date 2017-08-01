package com.ctrip.xpipe.redis.meta.server.dcchange;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Resource;

import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomeBackupAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomePrimaryAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.FirstNewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;

/**
 * @author wenchao.meng
 *
 * Dec 13, 2016
 */
@Component
public class DefaultChangePrimaryDcAction implements ChangePrimaryDcAction{
	
	private static Logger logger = LoggerFactory.getLogger(DefaultChangePrimaryDcAction.class);

	@Resource(name = MetaServerContextConfig.CLIENT_POOL)
	private XpipeNettyClientKeyedObjectPool keyedObjectPool;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	@Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
	private ExecutorService executors;

	@Autowired
	private DcMetaCache  dcMetaCache;
	
	@Autowired
	private CurrentMetaManager currentMetaManager;
	
	@Autowired
	private SentinelManager sentinelManager;
	
	@Autowired
	private MultiDcService multiDcService;

	@Override
	public PrimaryDcChangeMessage changePrimaryDc(String clusterId, String shardId, String newPrimaryDc) {
		
		if(!currentMetaManager.hasCluster(clusterId)){
			logger.info("[changePrimaryDc][not interested in this cluster]");
			return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.SUCCESS, "not interested in this cluster:" + clusterId);
		}
		
		ChangePrimaryDcAction changePrimaryDcAction = null;
		if(newPrimaryDc.equalsIgnoreCase(dcMetaCache.getCurrentDc())){
			logger.info("[doChangePrimaryDc][become primary]{}, {}", clusterId, shardId, newPrimaryDc);
			changePrimaryDcAction = new BecomePrimaryAction(dcMetaCache, currentMetaManager, sentinelManager, keyedObjectPool, createNewMasterChooser(), scheduled, executors);
		}else{
			logger.info("[doChangePrimaryDc][become backup]{}, {}", clusterId, shardId, newPrimaryDc);
			changePrimaryDcAction = new BecomeBackupAction(dcMetaCache, currentMetaManager, sentinelManager, keyedObjectPool, multiDcService, scheduled, executors);
		}
		return changePrimaryDcAction.changePrimaryDc(clusterId, shardId, newPrimaryDc);
	}

	private NewMasterChooser createNewMasterChooser() {
		return new FirstNewMasterChooser(keyedObjectPool, scheduled, executors);
	}

}
