package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomeBackupAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomePrimaryAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.FirstNewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

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

	@Autowired
	private OffsetWaiter offsetWaiter;

	@Autowired
	private CurrentClusterServer currentClusterServer;

	@Override
	public PrimaryDcChangeMessage changePrimaryDc(String clusterId, String shardId, String newPrimaryDc, MasterInfo masterInfo) {
		
		if(!currentMetaManager.hasCluster(clusterId)){
			logger.info("[changePrimaryDc][not interested in this cluster]");
			return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.SUCCESS, "not interested in this cluster:" + clusterId);
		}
		
		ChangePrimaryDcAction changePrimaryDcAction = null;

		ExecutionLog executionLog = new ExecutionLog(String.format("meta server:%s", currentClusterServer.getClusterInfo()));
		if(newPrimaryDc.equalsIgnoreCase(dcMetaCache.getCurrentDc())){
			logger.info("[doChangePrimaryDc][become primary]{}, {}", clusterId, shardId, newPrimaryDc);
			changePrimaryDcAction = new BecomePrimaryAction(dcMetaCache, currentMetaManager, sentinelManager, offsetWaiter, executionLog, keyedObjectPool, createNewMasterChooser(), scheduled, executors);
		}else{
			logger.info("[doChangePrimaryDc][become backup]{}, {}", clusterId, shardId, newPrimaryDc);
			changePrimaryDcAction = new BecomeBackupAction(dcMetaCache, currentMetaManager, sentinelManager, executionLog, keyedObjectPool, multiDcService, scheduled, executors);
		}
		return changePrimaryDcAction.changePrimaryDc(clusterId, shardId, newPrimaryDc, masterInfo);
	}

	private NewMasterChooser createNewMasterChooser() {
		return new FirstNewMasterChooser(keyedObjectPool, scheduled, executors);
	}

}
