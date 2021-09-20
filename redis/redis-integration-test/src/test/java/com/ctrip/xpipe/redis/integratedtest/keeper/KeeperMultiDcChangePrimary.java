package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.OffsetWaiter;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomeBackupAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomePrimaryAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.FirstNewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class KeeperMultiDcChangePrimary extends AbstractKeeperIntegratedMultiDc{
	
	@Mock
	public DcMetaCache dcMetaCache;

	@Mock
	public CurrentMetaManager currentMetaManager;
	
	@Mock 
	private SentinelManager sentinelManager;
	
	@Mock
	private MultiDcService multiDcService;

	@Mock
	private OffsetWaiter offsetWaiter;
	
	private FirstNewMasterChooser newMasterChooser;
	
	@Before
	public void beforeKeeperMultiDcChangePrimary() throws Exception{
		
		newMasterChooser = new FirstNewMasterChooser(getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
		
	}

	@Test
	public void testChangePrimary() throws Exception{
		
		String primaryDc = getPrimaryDc();
		String backupDc = getBackupDc();
		//change backup to primary
		
		when(dcMetaCache.getShardRedises(getClusterId(), getShardId())).thenReturn(getDcRedises(backupDc, getClusterId(), getShardId()));
		when(currentMetaManager.getSurviveKeepers(getClusterId(), getShardId())).thenReturn(getDcKeepers(backupDc, getClusterId(), getShardId()));
		
		logger.info(remarkableMessage("[make dc primary]change dc primary to:" + backupDc));
		BecomePrimaryAction becomePrimaryAction = new BecomePrimaryAction(dcMetaCache, currentMetaManager, sentinelManager, offsetWaiter, new ExecutionLog(currentTestName()), getXpipeNettyClientKeyedObjectPool(), newMasterChooser, scheduled, executors);
		PrimaryDcChangeMessage message = becomePrimaryAction.changePrimaryDc(getClusterId(), getShardId(), backupDc, new MasterInfo());
		logger.info("{}", message);

		sleep(2000);
		
		logger.info(remarkableMessage("[make dc backup]change dc primary to:" + backupDc));
		when(dcMetaCache.getPrimaryDc(getClusterId(), getShardId())).thenReturn(backupDc);
		when(multiDcService.getActiveKeeper(backupDc, getClusterId(), getShardId())).thenReturn(getDcKeepers(backupDc, getClusterId(), getShardId()).get(0));
		
		when(dcMetaCache.getShardRedises(getClusterId(), getShardId())).thenReturn(getDcRedises(primaryDc, getClusterId(), getShardId()));
		when(currentMetaManager.getKeeperActive(getClusterId(), getShardId())).thenReturn(getKeeperActive(primaryDc));
		when(currentMetaManager.getSurviveKeepers(getClusterId(), getShardId())).thenReturn(getDcKeepers(primaryDc, getClusterId(), getShardId()));
		
		BecomeBackupAction becomeBackupAction = new BecomeBackupAction(dcMetaCache, currentMetaManager, sentinelManager, new ExecutionLog(currentTestName()), getXpipeNettyClientKeyedObjectPool(), multiDcService, scheduled, executors);
		message = becomeBackupAction.changePrimaryDc(getClusterId(), getShardId(), backupDc, new MasterInfo());
		logger.info("{}", message);

		sleep(2000);
		
		RedisMeta newRedisMaster = newMasterChooser.getLastChoosenMaster();
		List<RedisMeta> allRedises = getRedises(primaryDc);
		allRedises.addAll(getRedises(backupDc));
		allRedises.remove(newRedisMaster);

		logger.info("{}\n{}", newRedisMaster, allRedises);
		sendMesssageToMasterAndTest(newRedisMaster, allRedises);
	}

	@Override
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(1 << 20, 100, 100 * (1 << 20), 2000);
	}


}
